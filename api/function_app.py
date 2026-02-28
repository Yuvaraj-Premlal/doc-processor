import azure.functions as func
import json
import os
import uuid
import base64
import io
from datetime import datetime, timezone

from azure.storage.blob import BlobServiceClient
from azure.storage.queue import QueueClient

from azure.core.credentials import AzureKeyCredential
from azure.ai.documentintelligence import DocumentIntelligenceClient

# PDF split/extract/rotate
from pypdf import PdfReader, PdfWriter


app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

# Your classifier labels (must match exactly what the classifier returns)
TARGET_DOC_TYPES = [
    "CEVA",
    "ENTRY SUMMARY",
    "PARTS WORKSHEET",
]

# Map docType -> extraction model id
EXTRACTION_MODELS = {
    "CEVA": "ceva_invoice_model",
    "ENTRY SUMMARY": "entry-summary-v1",
    "PARTS WORKSHEET": "partsworksheet_model",
}

# Rotation fallback settings for PARTS WORKSHEET (picked PDFs can be rotated 90°)
ROTATION_FALLBACK_DEGREES = -90  # try rotating clockwise 270 to make it upright


# =========================
# Helpers
# =========================
def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def blob_service() -> BlobServiceClient:
    return BlobServiceClient.from_connection_string(os.environ["AzureWebJobsStorage"])


def queue_client() -> QueueClient:
    conn = os.environ["AzureWebJobsStorage"]
    qname = "jobs"
    qc = QueueClient.from_connection_string(conn, qname)
    try:
        qc.create_queue()
    except Exception:
        pass
    return qc


def di_client() -> DocumentIntelligenceClient:
    endpoint = os.environ["DI_ENDPOINT"]
    key = os.environ["DI_KEY"]
    return DocumentIntelligenceClient(endpoint=endpoint, credential=AzureKeyCredential(key))


def _jsonable(obj):
    """Convert SDK objects to JSON-serializable structure (best-effort)."""
    if obj is None:
        return None
    if isinstance(obj, (str, int, float, bool)):
        return obj
    if isinstance(obj, bytes):
        return base64.b64encode(obj).decode("utf-8")
    if isinstance(obj, (list, tuple)):
        return [_jsonable(x) for x in obj]
    if isinstance(obj, dict):
        return {k: _jsonable(v) for k, v in obj.items()}

    for attr in ("to_dict", "as_dict", "model_dump"):
        fn = getattr(obj, attr, None)
        if callable(fn):
            try:
                return _jsonable(fn())
            except Exception:
                pass

    d = getattr(obj, "__dict__", None)
    if isinstance(d, dict) and d:
        return {k: _jsonable(v) for k, v in d.items()}

    return str(obj)


def normalize_doctype(s: str | None) -> str:
    return (s or "").strip().upper()


def safe_slug(s: str) -> str:
    s = (s or "").strip().upper().replace(" ", "_")
    keep = []
    for ch in s:
        if ch.isalnum() or ch in ("_", "-", "."):
            keep.append(ch)
    return "".join(keep) or "UNKNOWN"


def split_pdf_to_single_page_pdfs(pdf_bytes: bytes) -> list[dict]:
    """
    Returns a list of items:
      { "pageNumber": 1-based, "pdfBytes": <single-page pdf bytes> }
    """
    reader = PdfReader(io.BytesIO(pdf_bytes))
    out: list[dict] = []

    for i in range(len(reader.pages)):
        writer = PdfWriter()
        writer.add_page(reader.pages[i])
        buf = io.BytesIO()
        writer.write(buf)
        out.append({"pageNumber": i + 1, "pdfBytes": buf.getvalue()})

    return out


def extract_single_page_pdf(bundle_reader: PdfReader, page_number_1_based: int) -> bytes:
    """Extract one page (1-based) from a PdfReader and return as PDF bytes."""
    idx = page_number_1_based - 1
    if idx < 0 or idx >= len(bundle_reader.pages):
        raise ValueError(f"page {page_number_1_based} out of range (1..{len(bundle_reader.pages)})")

    writer = PdfWriter()
    writer.add_page(bundle_reader.pages[idx])
    buf = io.BytesIO()
    writer.write(buf)
    return buf.getvalue()


def rotate_pdf_bytes(pdf_bytes: bytes, degrees: int) -> bytes:
    """
    Rotate all pages in a PDF by degrees (must be multiple of 90).
    degrees: +90, +180, +270, -90, etc.
    """
    reader = PdfReader(io.BytesIO(pdf_bytes))
    writer = PdfWriter()

    # Normalize degrees to [0, 90, 180, 270]
    deg = degrees % 360
    for p in reader.pages:
        # pypdf supports rotate_clockwise / rotate_counter_clockwise
        if deg == 90:
            p.rotate_clockwise(90)
        elif deg == 180:
            p.rotate_clockwise(180)
        elif deg == 270:
            p.rotate_clockwise(270)
        writer.add_page(p)

    buf = io.BytesIO()
    writer.write(buf)
    return buf.getvalue()


def _field_value_simple(field_obj):
    """
    Best-effort conversion of DocumentField-like object into a simple JSON value.
    Prefer structured value if present, else content.
    """
    if field_obj is None:
        return None

    # DocumentField has 'value' for many types in newer SDKs; fall back to content
    v = getattr(field_obj, "value", None)
    if v is not None:
        # value can be primitive, list, dict, etc.
        return _jsonable(v)

    content = getattr(field_obj, "content", None)
    if content is not None:
        return content

    return _jsonable(field_obj)


def simplify_analyze_result(result):
    """
    Convert analyze result into a simple dict of extracted fields.
    Assumes first document is the relevant one.
    """
    docs = getattr(result, "documents", None) or []
    if not docs:
        return {"fields": {}, "docType": None, "confidence": None, "hasData": False}

    d0 = docs[0]
    doc_type = getattr(d0, "doc_type", None) or getattr(d0, "docType", None)
    conf = getattr(d0, "confidence", None)
    fields = getattr(d0, "fields", None) or {}

    simple_fields = {}
    for k, f in fields.items():
        simple_fields[str(k)] = _field_value_simple(f)

    has_data = len(simple_fields) > 0 and any(v not in (None, "", [], {}) for v in simple_fields.values())
    return {"fields": simple_fields, "docType": doc_type, "confidence": conf, "hasData": has_data}


def flatten_for_row(prefix: str, obj, out: dict):
    """
    Flatten nested dict/list into a single-level dict suitable for 1-row Excel later.
    Keys become prefix + '.' + nestedKey (lists become index-based).
    """
    if obj is None:
        return
    if isinstance(obj, (str, int, float, bool)):
        out[prefix] = obj
        return
    if isinstance(obj, dict):
        for k, v in obj.items():
            flatten_for_row(f"{prefix}.{k}", v, out)
        return
    if isinstance(obj, list):
        for i, v in enumerate(obj):
            flatten_for_row(f"{prefix}[{i}]", v, out)
        return
    # fallback
    out[prefix] = str(obj)


# =========================
# HTTP Upload Endpoint
# =========================
@app.route(route="upload", methods=["POST"])
def upload(req: func.HttpRequest) -> func.HttpResponse:
    try:
        if not hasattr(req, "files") or req.files is None:
            return func.HttpResponse(
                json.dumps({"error": "Send multipart/form-data with field name 'files'."}),
                status_code=400,
                mimetype="application/json",
            )

        files = req.files.getlist("files")
        if not files:
            return func.HttpResponse(
                json.dumps({"error": "No files uploaded. Use field name 'files'."}),
                status_code=400,
                mimetype="application/json",
            )

        job_id = str(uuid.uuid4())
        uploads_container = os.environ.get("UPLOADS_CONTAINER", "uploads")

        bsc = blob_service()
        container = bsc.get_container_client(uploads_container)
        try:
            container.create_container()
        except Exception:
            pass

        qc = queue_client()

        uploaded = []
        enqueued = []

        for f in files:
            filename = getattr(f, "filename", "file.pdf")
            blob_path = f"{job_id}/original/{filename}"

            # IMPORTANT: read full bytes (avoids truncated uploads)
            f.stream.seek(0)
            data = f.stream.read()
            container.upload_blob(blob_path, data, overwrite=True)
            print(f"[upload] uploaded {filename} bytes={len(data)}")

            uploaded.append({"file": filename, "blob": blob_path})

            msg = {"jobId": job_id, "pdfBlobPath": blob_path, "fileName": filename}
            qc.send_message(json.dumps(msg))
            enqueued.append(msg)

        return func.HttpResponse(
            json.dumps({"jobId": job_id, "files": uploaded, "queued": enqueued}),
            mimetype="application/json",
        )

    except Exception as e:
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            status_code=500,
            mimetype="application/json",
        )


# =========================
# Queue Worker:
#   Bundle -> split pages -> classify each page
#   -> best page per doc type -> save picked single-page PDFs
#   -> run extraction per picked pdf -> merge into 1-row table.json
# =========================
@app.function_name(name="job_worker")
@app.queue_trigger(
    arg_name="msg",
    queue_name="jobs",
    connection="AzureWebJobsStorage",
)
def job_worker(msg: func.QueueMessage) -> None:
    raw = msg.get_body().decode("utf-8")

    try:
        payload = json.loads(raw)
    except Exception:
        payload = {"raw": raw}

    job_id = payload.get("jobId", "unknown")
    pdf_blob_path = payload.get("pdfBlobPath", "")

    print(f"[worker] Received jobId={job_id} pdfBlobPath={pdf_blob_path}")

    uploads_container = os.environ.get("UPLOADS_CONTAINER", "uploads")
    results_container = os.environ.get("RESULTS_CONTAINER", "results")

    bsc = blob_service()
    uc = bsc.get_container_client(uploads_container)
    rc = bsc.get_container_client(results_container)

    try:
        rc.create_container()
    except Exception:
        pass

    status_blob = f"{job_id}/status.json"

    def write_status(stage: str, extra: dict | None = None):
        status = {
            "jobId": job_id,
            "stage": stage,
            "pdfBlobPath": pdf_blob_path,
            "updatedAt": utc_now_iso(),
        }
        if extra:
            status.update(extra)
        rc.upload_blob(status_blob, json.dumps(status), overwrite=True)
        print(f"[worker] status => {stage}")

    try:
        write_status("page_classifying")

        if not pdf_blob_path:
            raise ValueError("pdfBlobPath missing in queue payload.")

        # 1) Download bundle PDF
        pdf_bytes = uc.get_blob_client(pdf_blob_path).download_blob().readall()
        print(f"[worker] Downloaded bundle bytes={len(pdf_bytes)}")

        bundle_reader = PdfReader(io.BytesIO(pdf_bytes))
        total_pages = len(bundle_reader.pages)
        print(f"[worker] Bundle pages={total_pages}")

        # 2) Split into single-page PDFs (in-memory)
        pages = split_pdf_to_single_page_pdfs(pdf_bytes)

        # 3) Classify each page
        classifier_id = "cevadocclassifier"  # your classifier ID
        client = di_client()

        page_results: list[dict] = []
        failures: list[dict] = []

        for item in pages:
            pno = item["pageNumber"]
            one_page_pdf = item["pdfBytes"]

            try:
                poller = client.begin_classify_document(
                    classifier_id,
                    body=one_page_pdf,
                    content_type="application/pdf",
                )
                result = poller.result()

                docs = getattr(result, "documents", None) or []
                if docs:
                    d0 = docs[0]
                    doc_type = getattr(d0, "doc_type", None) or getattr(d0, "docType", None)
                    conf = getattr(d0, "confidence", None)
                else:
                    doc_type = None
                    conf = None

                page_results.append(
                    {"pageNumber": pno, "predictedDocType": doc_type, "confidence": conf}
                )
                print(f"[worker] page={pno} => {doc_type} (conf={conf})")

            except Exception as e:
                failures.append({"pageNumber": pno, "error": str(e)})
                page_results.append(
                    {"pageNumber": pno, "predictedDocType": None, "confidence": None, "error": str(e)}
                )
                print(f"[worker][WARN] page={pno} classify failed: {str(e)}")

        # 4) Choose best page (max confidence) for each target doc type (no ranges)
        best_pages: dict = {}
        for target in TARGET_DOC_TYPES:
            target_norm = normalize_doctype(target)
            candidates = [
                r for r in page_results
                if normalize_doctype(r.get("predictedDocType")) == target_norm
                and isinstance(r.get("confidence"), (int, float))
            ]
            candidates.sort(key=lambda x: float(x["confidence"]), reverse=True)

            best = candidates[0] if candidates else None
            best_pages[target] = {
                "bestPageNumber": best["pageNumber"] if best else None,
                "bestConfidence": float(best["confidence"]) if best else None,
                "topPages": [
                    {"pageNumber": c["pageNumber"], "confidence": float(c["confidence"])}
                    for c in candidates[:3]
                ],
            }

        # 5) Save JSON outputs for classification
        page_scores_blob = f"{job_id}/page_classification.json"
        best_pages_blob = f"{job_id}/best_pages.json"

        page_payload = {
            "jobId": job_id,
            "classifierId": classifier_id,
            "createdAt": utc_now_iso(),
            "bundleDiagnostics": {
                "downloadedBytes": len(pdf_bytes),
                "totalPages": total_pages,
                "failedPages": len(failures),
                "failures": failures,
            },
            "pageResults": page_results,
        }
        rc.upload_blob(page_scores_blob, json.dumps(page_payload), overwrite=True)

        best_payload = {
            "jobId": job_id,
            "classifierId": classifier_id,
            "createdAt": utc_now_iso(),
            "targets": TARGET_DOC_TYPES,
            "bestPages": best_pages,
        }
        rc.upload_blob(best_pages_blob, json.dumps(best_payload), overwrite=True)

        print(f"[worker] Wrote {results_container}/{page_scores_blob}")
        print(f"[worker] Wrote {results_container}/{best_pages_blob}")

        # 6) Extract & save the best pages as single-page PDFs
        write_status("pages_extracting", {"totalPages": total_pages, "failedPages": len(failures)})

        picked_outputs = []
        for target in TARGET_DOC_TYPES:
            bp = best_pages.get(target, {})
            page_no = bp.get("bestPageNumber")

            if not isinstance(page_no, int):
                picked_outputs.append(
                    {
                        "docType": target,
                        "pickedPage": None,
                        "confidence": bp.get("bestConfidence"),
                        "blob": None,
                        "note": "No matching page found",
                    }
                )
                continue

            picked_pdf_bytes = extract_single_page_pdf(bundle_reader, page_no)
            blob_name = f"{job_id}/picked/{safe_slug(target)}_page_{page_no}.pdf"
            rc.upload_blob(blob_name, picked_pdf_bytes, overwrite=True)

            picked_outputs.append(
                {
                    "docType": target,
                    "pickedPage": page_no,
                    "confidence": bp.get("bestConfidence"),
                    "blob": blob_name,
                    "bytes": len(picked_pdf_bytes),
                }
            )
            print(f"[worker] Picked {target} => page {page_no} -> {results_container}/{blob_name}")

        picked_blob = f"{job_id}/picked_pages.json"
        picked_payload = {
            "jobId": job_id,
            "classifierId": classifier_id,
            "createdAt": utc_now_iso(),
            "picked": picked_outputs,
        }
        rc.upload_blob(picked_blob, json.dumps(picked_payload), overwrite=True)
        print(f"[worker] Wrote {results_container}/{picked_blob}")

        # 7) Run extraction models on picked PDFs
        write_status("extracting_models")

        extracted_outputs = []
        merged_row = {
            "jobId": job_id,
            "sourcePdfBlobPath": pdf_blob_path,
            "createdAt": utc_now_iso(),
            "pickedPages": {p["docType"]: p["pickedPage"] for p in picked_outputs if p.get("pickedPage")},
            "confidence": {p["docType"]: p.get("confidence") for p in picked_outputs if p.get("confidence") is not None},
        }

        flattened = {}
        # Add core info into flattened too (nice for 1-row Excel later)
        flattened["jobId"] = job_id
        flattened["sourcePdfBlobPath"] = pdf_blob_path
        for dt, pn in merged_row.get("pickedPages", {}).items():
            flattened[f"picked.{safe_slug(dt)}.page"] = pn
        for dt, cf in merged_row.get("confidence", {}).items():
            flattened[f"picked.{safe_slug(dt)}.confidence"] = cf

        for picked in picked_outputs:
            doc_type = picked.get("docType")
            picked_blob_path = picked.get("blob")
            picked_page = picked.get("pickedPage")

            if not picked_blob_path or not picked_page:
                extracted_outputs.append(
                    {
                        "docType": doc_type,
                        "modelId": EXTRACTION_MODELS.get(doc_type),
                        "pickedBlob": picked_blob_path,
                        "pickedPage": picked_page,
                        "status": "skipped",
                        "reason": "no picked page",
                    }
                )
                continue

            model_id = EXTRACTION_MODELS.get(doc_type)
            if not model_id:
                extracted_outputs.append(
                    {
                        "docType": doc_type,
                        "modelId": None,
                        "pickedBlob": picked_blob_path,
                        "pickedPage": picked_page,
                        "status": "skipped",
                        "reason": "no model mapping",
                    }
                )
                continue

            # Download picked PDF bytes from results container
            picked_pdf_bytes = rc.get_blob_client(picked_blob_path).download_blob().readall()

            def analyze_with_model(pdf_bytes_to_use: bytes):
                poller = client.begin_analyze_document(
                    model_id,
                    body=pdf_bytes_to_use,
                    content_type="application/pdf",
                )
                return poller.result()

            # First attempt
            analyze_result = analyze_with_model(picked_pdf_bytes)
            simplified = simplify_analyze_result(analyze_result)

            used_rotation = 0

            # Rotation fallback only for PARTS WORKSHEET if extraction seems empty
            if normalize_doctype(doc_type) == "PARTS WORKSHEET" and not simplified.get("hasData", False):
                try:
                    rotated_bytes = rotate_pdf_bytes(picked_pdf_bytes, ROTATION_FALLBACK_DEGREES)
                    analyze_result_2 = analyze_with_model(rotated_bytes)
                    simplified_2 = simplify_analyze_result(analyze_result_2)

                    # Keep the one with more data
                    if simplified_2.get("hasData", False) and len(simplified_2.get("fields", {})) >= len(simplified.get("fields", {})):
                        analyze_result = analyze_result_2
                        simplified = simplified_2
                        used_rotation = ROTATION_FALLBACK_DEGREES
                        # also overwrite a rotated copy for your inspection (optional)
                        rotated_blob = f"{job_id}/picked/{safe_slug(doc_type)}_page_{picked_page}_ROTATED.pdf"
                        rc.upload_blob(rotated_blob, rotated_bytes, overwrite=True)
                        print(f"[worker] Rotation fallback used for {doc_type}; wrote {results_container}/{rotated_blob}")
                except Exception as rot_e:
                    print(f"[worker][WARN] Rotation fallback failed for {doc_type}: {str(rot_e)}")

            # Save extraction output
            extracted_blob = f"{job_id}/extracted/{safe_slug(doc_type)}.json"
            extracted_payload = {
                "jobId": job_id,
                "docType": doc_type,
                "modelId": model_id,
                "pickedBlob": picked_blob_path,
                "pickedPage": picked_page,
                "pickedConfidence": picked.get("confidence"),
                "createdAt": utc_now_iso(),
                "rotationAppliedDegrees": used_rotation,
                "simplified": simplified,
                "raw": _jsonable(analyze_result),
            }
            rc.upload_blob(extracted_blob, json.dumps(extracted_payload), overwrite=True)
            print(f"[worker] Wrote {results_container}/{extracted_blob}")

            extracted_outputs.append(
                {
                    "docType": doc_type,
                    "modelId": model_id,
                    "pickedBlob": picked_blob_path,
                    "pickedPage": picked_page,
                    "rotationAppliedDegrees": used_rotation,
                    "extractedBlob": extracted_blob,
                    "fieldCount": len(simplified.get("fields", {})),
                }
            )

            # Merge into 1 row per bundle
            # Store nested sections AND a flattened version
            section_key = safe_slug(doc_type).lower()  # ceva, entry_summary, parts_worksheet
            merged_row[section_key] = simplified.get("fields", {})

            # Flatten with prefix
            flatten_for_row(section_key, simplified.get("fields", {}), flattened)

        # Save merged table.json (1 row per bundle)
        merged_row["extracted"] = extracted_outputs
        merged_row["flattened"] = flattened

        table_blob = f"{job_id}/table.json"
        rc.upload_blob(table_blob, json.dumps(merged_row), overwrite=True)
        print(f"[worker] Wrote {results_container}/{table_blob}")

        # Final status
        write_status(
            "extracted_merged",
            {
                "totalPages": total_pages,
                "targets": TARGET_DOC_TYPES,
                "outputs": {
                    "pageClassification": f"{results_container}/{page_scores_blob}",
                    "bestPages": f"{results_container}/{best_pages_blob}",
                    "pickedPages": f"{results_container}/{picked_blob}",
                    "pickedPrefix": f"{results_container}/{job_id}/picked/",
                    "table": f"{results_container}/{table_blob}",
                    "extractedPrefix": f"{results_container}/{job_id}/extracted/",
                },
                "failedPages": len(failures),
                "extractedDocs": extracted_outputs,
            },
        )

    except Exception as e:
        print(f"[worker][ERROR] {str(e)}")
        write_status("failed", {"error": str(e)})
        return
# =========================
# HTTP: Get UI-friendly job output
# =========================
@app.route(route="job/{jobId}/ui", methods=["GET"])
def get_job_ui(req: func.HttpRequest) -> func.HttpResponse:
    job_id = req.route_params.get("jobId")

    try:
        results_container = os.environ.get("RESULTS_CONTAINER", "results")
        rc = blob_service().get_container_client(results_container)

        blob_path = f"{job_id}/table.json"
        blob_client = rc.get_blob_client(blob_path)

        if not blob_client.exists():
            return func.HttpResponse(
                json.dumps({"error": "Not ready"}),
                status_code=404,
                mimetype="application/json"
            )

        data = json.loads(blob_client.download_blob().readall())

        # ---- Simplify + remove geometry ----
        def simplify(value):
            if isinstance(value, dict):

                # unwrap DI typed fields
                if "valueString" in value:
                    return value["valueString"]
                if "valueNumber" in value:
                    return value["valueNumber"]
                if "valueArray" in value:
                    return [simplify(v.get("valueObject", v)) for v in value["valueArray"]]
                if "valueObject" in value:
                    return {k: simplify(v) for k, v in value["valueObject"].items()}

                # remove geometry
                clean = {}
                for k, v in value.items():
                    if k in ["boundingRegions", "polygon", "spans", "content"]:
                        continue
                    clean[k] = simplify(v)
                return clean

            if isinstance(value, list):
                return [simplify(v) for v in value]

            return value

        ui = {
            "jobId": data.get("jobId"),
            "pickedPages": data.get("pickedPages"),
            "confidence": data.get("confidence"),
            "ceva": simplify(data.get("ceva")),
            "entry_summary": simplify(data.get("entry_summary")),
            "parts_worksheet": simplify(data.get("parts_worksheet")),
        }

        return func.HttpResponse(
            json.dumps(ui),
            status_code=200,
            mimetype="application/json"
        )

    except Exception as e:
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            status_code=500,
            mimetype="application/json"
        )
