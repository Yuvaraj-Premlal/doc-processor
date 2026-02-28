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

# PDF split/extract
from pypdf import PdfReader, PdfWriter


app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

# Your classifier labels (must match exactly what the classifier returns)
TARGET_DOC_TYPES = [
    "CEVA",
    "ENTRY SUMMARY",
    "PARTS WORKSHEET",
]


# =========================
# Helpers
# =========================
def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def blob_service() -> BlobServiceClient:
    return BlobServiceClient.from_connection_string(os.environ["AzureWebJobsStorage"])


def queue_client() -> QueueClient:
    # Hardcoded to avoid extra configuration
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

        # 5) Save JSON outputs
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

        # 7) Final status for this step
        write_status(
            "pages_extracted",
            {
                "totalPages": total_pages,
                "targets": TARGET_DOC_TYPES,
                "outputs": {
                    "pageClassification": f"{results_container}/{page_scores_blob}",
                    "bestPages": f"{results_container}/{best_pages_blob}",
                    "pickedPages": f"{results_container}/{picked_blob}",
                    "pickedPrefix": f"{results_container}/{job_id}/picked/",
                },
                "failedPages": len(failures),
            },
        )

    except Exception as e:
        print(f"[worker][ERROR] {str(e)}")
        write_status("failed", {"error": str(e)})
        return
