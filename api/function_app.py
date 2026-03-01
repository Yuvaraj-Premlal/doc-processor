import azure.functions as func
import json
import os
import uuid
import base64
import io
import zipfile
from datetime import datetime, timezone, timedelta
from typing import Optional, Tuple, Any, Dict, List

from azure.storage.blob import BlobServiceClient
from azure.storage.queue import QueueClient

from azure.core.credentials import AzureKeyCredential
from azure.ai.documentintelligence import DocumentIntelligenceClient

from azure.cosmos import CosmosClient
from azure.cosmos.exceptions import CosmosResourceNotFoundError

from pypdf import PdfReader, PdfWriter

from openpyxl import Workbook
from openpyxl.utils import get_column_letter


app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

# =========================
# Config
# =========================

TARGET_DOC_TYPES = ["CEVA", "ENTRY SUMMARY", "PARTS WORKSHEET"]

EXTRACTION_MODELS = {
    "CEVA": "ceva_invoice_model",
    "ENTRY SUMMARY": "entry-summary-v1",
    "PARTS WORKSHEET": "partsworksheet_model",
}

ROTATION_FALLBACK_DEGREES = -90

UPLOADS_CONTAINER = os.environ.get("UPLOADS_CONTAINER", "uploads")
QUEUE_NAME = os.environ.get("JOBS_QUEUE", "jobs")

DEFAULT_PAGE_SIZE = 20
MAX_PAGE_SIZE = 200

CEVA_INVOICE_DATE_FIELD = "INVOICE DATE"


# =========================
# Core helpers
# =========================

def utc_now() -> datetime:
    return datetime.now(timezone.utc)

def utc_now_iso() -> str:
    return utc_now().isoformat()

def blob_service() -> BlobServiceClient:
    return BlobServiceClient.from_connection_string(os.environ["AzureWebJobsStorage"])

def queue_client() -> QueueClient:
    qc = QueueClient.from_connection_string(os.environ["AzureWebJobsStorage"], QUEUE_NAME)
    try:
        qc.create_queue()
    except Exception:
        pass
    return qc

def di_client() -> DocumentIntelligenceClient:
    return DocumentIntelligenceClient(
        endpoint=os.environ["DI_ENDPOINT"],
        credential=AzureKeyCredential(os.environ["DI_KEY"]),
    )

def cosmos_container():
    client = CosmosClient(os.environ["COSMOS_ENDPOINT"], credential=os.environ["COSMOS_KEY"])
    db = client.get_database_client(os.environ.get("COSMOS_DATABASE", "pdfbundle"))
    return db.get_container_client(os.environ.get("COSMOS_CONTAINER", "results"))

def delete_prefix(container_client, prefix: str):
    for blob in container_client.list_blobs(name_starts_with=prefix):
        container_client.delete_blob(blob.name)


# =========================
# Utility helpers
# =========================

def normalize_doctype(s: Optional[str]) -> str:
    return (s or "").strip().upper()

def safe_slug(s: str) -> str:
    return (s or "").strip().upper().replace(" ", "_")

def _is_pdf_filename(name: str) -> bool:
    return (name or "").lower().endswith(".pdf")

def _is_zip_filename(name: str) -> bool:
    return (name or "").lower().endswith(".zip")

def rotate_pdf_bytes(pdf_bytes: bytes, degrees: int) -> bytes:
    reader = PdfReader(io.BytesIO(pdf_bytes))
    writer = PdfWriter()
    deg = degrees % 360
    for p in reader.pages:
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

def _jsonable(obj):
    if obj is None:
        return None
    if isinstance(obj, (str, int, float, bool)):
        return obj
    if isinstance(obj, bytes):
        return base64.b64encode(obj).decode("utf-8")
    if isinstance(obj, list):
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
    return str(obj)

def _field_value_simple(field):
    if field is None:
        return None
    value = getattr(field, "value", None)
    if value is not None:
        return _jsonable(value)
    content = getattr(field, "content", None)
    if content is not None:
        return content
    return _jsonable(field)

def simplify_analyze_result(result):
    docs = getattr(result, "documents", None) or []
    if not docs:
        return {"fields": {}, "hasData": False}

    fields = getattr(docs[0], "fields", None) or {}
    simple = {}
    for k, v in fields.items():
        simple[str(k)] = _field_value_simple(v)

    has_data = any(v not in (None, "", [], {}) for v in simple.values())
    return {"fields": simple, "hasData": has_data}


# -------------------------
# DI-wrapper unwrapping (THIS fixes [object Object])
# -------------------------

def unwrap_di_value(v: Any) -> Any:
    """
    Turns DI JSON-ish wrapper shapes into plain JSON:
      - {"valueString": "..."} -> "..."
      - {"valueNumber": 1.2} -> 1.2
      - {"valueArray":[{"valueObject":{...}}]} -> [ {..}, ... ]
      - {"valueObject":{k:v}} -> {k: unwrap(v)}
      - keeps dict/list recursively
    """
    if v is None:
        return None

    if isinstance(v, (str, int, float, bool)):
        return v

    if isinstance(v, list):
        return [unwrap_di_value(x) for x in v]

    if isinstance(v, dict):
        # common typed wrappers
        if "valueString" in v:
            return v.get("valueString")
        if "valueNumber" in v:
            return v.get("valueNumber")
        if "valueInteger" in v:
            return v.get("valueInteger")
        if "valueDate" in v:
            return v.get("valueDate")
        if "valueTime" in v:
            return v.get("valueTime")
        if "valuePhoneNumber" in v:
            return v.get("valuePhoneNumber")
        if "valueArray" in v:
            arr = v.get("valueArray") or []
            out = []
            for it in arr:
                if isinstance(it, dict) and "valueObject" in it:
                    out.append(unwrap_di_value(it.get("valueObject")))
                else:
                    out.append(unwrap_di_value(it))
            return out
        if "valueObject" in v:
            obj = v.get("valueObject") or {}
            return {k: unwrap_di_value(val) for k, val in obj.items()}

        # sometimes DI gives {"type":"array","valueArray":[...]} or nested objects
        if "type" in v and "valueArray" in v:
            return unwrap_di_value({"valueArray": v.get("valueArray")})
        if "type" in v and "valueObject" in v:
            return unwrap_di_value({"valueObject": v.get("valueObject")})

        # fallback recurse
        drop_keys = {"boundingRegions", "polygon", "spans"}  # noise
        out = {}
        for k, val in v.items():
            if k in drop_keys:
                continue
            out[k] = unwrap_di_value(val)
        return out

    return str(v)

def normalize_parts_worksheet_fields(fields: Dict[str, Any]) -> Dict[str, Any]:
    """
    Ensures:
      parts_worksheet.Supplier -> string
      parts_worksheet.LineItems -> list[dict] (NOT DI wrapper)
    """
    out = dict(fields or {})
    supplier = out.get("Supplier")
    out["Supplier"] = unwrap_di_value(supplier)

    li = out.get("LineItems")
    li_unwrapped = unwrap_di_value(li)

    # li_unwrapped should now be:
    # - list[dict] OR None
    if isinstance(li_unwrapped, list):
        # ensure each item is dict
        cleaned_items = []
        for item in li_unwrapped:
            if isinstance(item, dict):
                cleaned_items.append({k: unwrap_di_value(v) for k, v in item.items()})
            else:
                # if it’s primitive, keep as string
                cleaned_items.append({"value": unwrap_di_value(item)})
        out["LineItems"] = cleaned_items
    else:
        out["LineItems"] = []

    return out


# -------------------------
# Flatten for Excel (1-row)
# -------------------------

def flatten_for_row(prefix, obj, out: dict):
    if obj is None:
        return
    if isinstance(obj, (str, int, float, bool)):
        out[prefix] = obj
    elif isinstance(obj, dict):
        for k, v in obj.items():
            flatten_for_row(f"{prefix}.{k}", v, out)
    elif isinstance(obj, list):
        # store lists as joined string for the flat sheet
        out[prefix] = " | ".join([str(x) for x in obj])
    else:
        out[prefix] = str(obj)


# -------------------------
# Invoice date parsing / filters
# -------------------------

def _parse_iso_date_only(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    s = s.strip()
    if len(s) >= 10:
        d = s[:10]
        if len(d) == 10 and d[4] == "-" and d[7] == "-":
            return d
    return None

def _try_parse_date_to_yyyy_mm_dd(value: Any) -> Optional[str]:
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None

    if len(s) >= 10 and s[4] == "-" and s[7] == "-":
        return s[:10]

    s2 = s.replace(".", "/").replace("-", "/")
    parts = [p.strip() for p in s2.split("/") if p.strip()]

    if len(parts) == 3 and len(parts[0]) == 4:
        y, m, d = parts[0], parts[1].zfill(2), parts[2].zfill(2)
        if y.isdigit() and m.isdigit() and d.isdigit():
            return f"{y}-{m}-{d}"

    if len(parts) == 3 and len(parts[2]) == 4 and parts[2].isdigit():
        a, b, y = parts[0], parts[1], parts[2]
        if a.isdigit() and b.isdigit():
            da = int(a); db = int(b)
            if da > 12:
                d, m = da, db
            elif db > 12:
                m, d = da, db
            else:
                d, m = da, db
            return f"{y}-{str(m).zfill(2)}-{str(d).zfill(2)}"

    try:
        t = s.replace("-", " ").replace("/", " ")
        dt = datetime.strptime(t.strip(), "%d %b %Y")
        return dt.strftime("%Y-%m-%d")
    except Exception:
        pass

    try:
        t = s.replace("-", " ").replace("/", " ")
        dt = datetime.strptime(t.strip(), "%d %B %Y")
        return dt.strftime("%Y-%m-%d")
    except Exception:
        pass

    return None

def _extract_ceva_invoice_date(doc: dict) -> Optional[str]:
    ceva = doc.get("ceva") if isinstance(doc.get("ceva"), dict) else {}
    if CEVA_INVOICE_DATE_FIELD in ceva:
        return _try_parse_date_to_yyyy_mm_dd(ceva.get(CEVA_INVOICE_DATE_FIELD))
    for k, v in ceva.items():
        lk = str(k).lower()
        if "invoice" in lk and "date" in lk:
            parsed = _try_parse_date_to_yyyy_mm_dd(v)
            if parsed:
                return parsed
    return None

def _compute_range(range_key: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    if not range_key:
        return (None, None)
    rk = range_key.strip().lower()
    today = utc_now().date()

    if rk == "all":
        return (None, None)
    if rk in ("last_7_days", "last_week"):
        start = today - timedelta(days=7)
        return (start.strftime("%Y-%m-%d"), today.strftime("%Y-%m-%d"))
    if rk == "last_30_days":
        start = today - timedelta(days=30)
        return (start.strftime("%Y-%m-%d"), today.strftime("%Y-%m-%d"))
    if rk == "last_month":
        first_this_month = today.replace(day=1)
        last_prev_month = first_this_month - timedelta(days=1)
        first_prev_month = last_prev_month.replace(day=1)
        return (first_prev_month.strftime("%Y-%m-%d"), last_prev_month.strftime("%Y-%m-%d"))
    return (None, None)


# =========================
# Upload Endpoint (PDF(s) + ZIP)
# =========================

@app.route(route="upload", methods=["POST"])
def upload(req: func.HttpRequest) -> func.HttpResponse:
    files = req.files.getlist("files")
    if not files:
        return func.HttpResponse(
            json.dumps({"error": "Upload via multipart/form-data field 'files'"}),
            status_code=400,
            mimetype="application/json",
        )

    bsc = blob_service()
    uploads = bsc.get_container_client(UPLOADS_CONTAINER)
    try:
        uploads.create_container()
    except Exception:
        pass

    qc = queue_client()

    batch_id = str(uuid.uuid4())
    jobs = []

    def enqueue_one_pdf(pdf_bytes: bytes, filename: str) -> dict:
        job_id = str(uuid.uuid4())
        blob_path = f"{job_id}/original/{filename}"
        uploads.upload_blob(blob_path, pdf_bytes, overwrite=True)
        qc.send_message(json.dumps({"jobId": job_id, "pdfBlobPath": blob_path, "fileName": filename}))
        return {"jobId": job_id, "fileName": filename}

    try:
        for f in files:
            filename = getattr(f, "filename", "file.bin")
            f.stream.seek(0)
            data = f.stream.read()

            if _is_zip_filename(filename):
                with zipfile.ZipFile(io.BytesIO(data), "r") as z:
                    for zi in z.infolist():
                        if zi.is_dir():
                            continue
                        inner = zi.filename
                        if not _is_pdf_filename(inner):
                            continue
                        pdf_bytes = z.read(zi)
                        leaf = inner.split("/")[-1].split("\\")[-1] or "file.pdf"
                        jobs.append(enqueue_one_pdf(pdf_bytes, leaf))

            elif _is_pdf_filename(filename):
                jobs.append(enqueue_one_pdf(data, filename))

            else:
                return func.HttpResponse(
                    json.dumps({"error": f"Unsupported file type: {filename}. Upload PDF or ZIP containing PDFs."}),
                    status_code=400,
                    mimetype="application/json",
                )

        resp = {"batchId": batch_id, "jobs": jobs}
        if len(jobs) == 1:
            resp["jobId"] = jobs[0]["jobId"]

        return func.HttpResponse(json.dumps(resp), status_code=200, mimetype="application/json")

    except zipfile.BadZipFile:
        return func.HttpResponse(json.dumps({"error": "Invalid ZIP file."}), status_code=400, mimetype="application/json")
    except Exception as e:
        return func.HttpResponse(json.dumps({"error": str(e)}), status_code=500, mimetype="application/json")


# =========================
# Queue Worker
# =========================

@app.function_name(name="job_worker")
@app.queue_trigger(arg_name="msg", queue_name=QUEUE_NAME, connection="AzureWebJobsStorage")
def job_worker(msg: func.QueueMessage) -> None:
    payload = json.loads(msg.get_body().decode("utf-8"))

    job_id = payload.get("jobId")
    pdf_blob_path = payload.get("pdfBlobPath")
    file_name = payload.get("fileName")

    container = cosmos_container()
    uploads = blob_service().get_container_client(UPLOADS_CONTAINER)

    created_at = utc_now_iso()

    try:
        # mark processing
        container.upsert_item({
            "id": job_id,
            "type": "bundle_result",
            "fileName": file_name,
            "createdAt": created_at,
            "status": "processing",
        })

        if not job_id or not pdf_blob_path:
            raise ValueError("Queue payload missing jobId or pdfBlobPath")

        pdf_bytes = uploads.get_blob_client(pdf_blob_path).download_blob().readall()
        reader = PdfReader(io.BytesIO(pdf_bytes))
        total_pages = len(reader.pages)

        client = di_client()

        page_results = []
        failures = []

        # -------- Classification per page --------
        for i, page in enumerate(reader.pages):
            writer = PdfWriter()
            writer.add_page(page)
            buf = io.BytesIO()
            writer.write(buf)
            one_page_pdf = buf.getvalue()

            try:
                poller = client.begin_classify_document(
                    "cevadocclassifier",
                    body=one_page_pdf,
                    content_type="application/pdf",
                )
                result = poller.result()
                docs = getattr(result, "documents", None) or []
                if docs:
                    d0 = docs[0]
                    doc_type = getattr(d0, "doc_type", None) or getattr(d0, "docType", None)
                    conf = getattr(d0, "confidence", None)
                    page_results.append({
                        "pageNumber": i + 1,
                        "docType": doc_type,
                        "confidence": conf,
                        "pdf": one_page_pdf
                    })
            except Exception as e:
                failures.append({"pageNumber": i + 1, "error": str(e)})

        # -------- Pick best page per target --------
        best_pages = {}
        for target in TARGET_DOC_TYPES:
            matches = [
                r for r in page_results
                if normalize_doctype(r.get("docType")) == normalize_doctype(target)
                and isinstance(r.get("confidence"), (int, float))
            ]
            matches.sort(key=lambda x: float(x["confidence"]), reverse=True)
            if matches:
                best_pages[target] = matches[0]

        merged = {}
        flattened = {}
        picked_pages = {}
        confidence = []
        extracted_docs = []

        # -------- Extraction --------
        for doc_type, info in best_pages.items():
            model_id = EXTRACTION_MODELS.get(doc_type)
            if not model_id:
                continue

            def analyze(pdf_bytes_to_use: bytes):
                poller = client.begin_analyze_document(
                    model_id,
                    body=pdf_bytes_to_use,
                    content_type="application/pdf",
                )
                return poller.result()

            analyze_result = analyze(info["pdf"])
            simplified = simplify_analyze_result(analyze_result)
            used_rotation = 0

            # rotation fallback only for parts worksheet
            if normalize_doctype(doc_type) == "PARTS WORKSHEET" and not simplified.get("hasData", False):
                try:
                    rotated = rotate_pdf_bytes(info["pdf"], ROTATION_FALLBACK_DEGREES)
                    analyze2 = analyze(rotated)
                    simplified2 = simplify_analyze_result(analyze2)
                    if simplified2.get("hasData", False) and len(simplified2.get("fields", {})) >= len(simplified.get("fields", {})):
                        simplified = simplified2
                        used_rotation = ROTATION_FALLBACK_DEGREES
                except Exception:
                    pass

            key = safe_slug(doc_type).lower()

            # normalize PARTS WORKSHEET -> Supplier:string, LineItems:list[dict]
            if normalize_doctype(doc_type) == "PARTS WORKSHEET":
                merged[key] = normalize_parts_worksheet_fields(simplified.get("fields", {}))
            else:
                merged[key] = unwrap_di_value(simplified.get("fields", {}))

            # flattened (good for export)
            flatten_for_row(key, merged[key], flattened)

            picked_pages[doc_type] = info.get("pageNumber")
            confidence.append((doc_type, info.get("confidence")))

            extracted_docs.append({
                "docType": doc_type,
                "modelId": model_id,
                "page": info.get("pageNumber"),
                "rotationAppliedDegrees": used_rotation,
                "fieldCount": len(simplified.get("fields", {})),
            })

        # confidence dict
        confidence_dict = {dt: float(cf) for dt, cf in confidence if cf is not None}

        final_doc = {
            "id": job_id,
            "type": "bundle_result",
            "fileName": file_name,
            "createdAt": created_at,
            "completedAt": utc_now_iso(),
            "status": "completed",
            "sourcePdfBlobPath": pdf_blob_path,
            "bundleDiagnostics": {
                "downloadedBytes": len(pdf_bytes),
                "totalPages": total_pages,
                "failedPages": len(failures),
                "failures": failures,
            },
            "pickedPages": picked_pages,
            "confidence": confidence_dict,
            "flattened": flattened,
            "extractedDocs": extracted_docs,
            **merged,
        }

        # Top-level queryable invoice date (YYYY-MM-DD)
        final_doc["cevaInvoiceDate"] = _extract_ceva_invoice_date(final_doc)

        # -------------------------
        # BUSINESS SUMMARY FIELDS (for table + excel)
        # -------------------------
        ceva = final_doc.get("ceva", {}) if isinstance(final_doc.get("ceva"), dict) else {}
        es = final_doc.get("entry_summary", {}) if isinstance(final_doc.get("entry_summary"), dict) else {}
        pw = final_doc.get("parts_worksheet", {}) if isinstance(final_doc.get("parts_worksheet"), dict) else {}

        final_doc["invoiceNumber"] = ceva.get("INVOICE NUMBER")
        final_doc["entryNumber"] = ceva.get("ENTRY NUMBER")
        final_doc["departureDate"] = ceva.get("DEPARTURE DATE")
        final_doc["arrivalDate"] = ceva.get("ARRIVAL DATE")
        final_doc["descriptionOfGoods"] = ceva.get("DESCRIPTION OF GOODS")

        final_doc["totalEnteredValue"] = es.get("Total_Entered_Value")
        final_doc["totalOtherFees"] = es.get("Total_Other_Fees")
        final_doc["duty"] = es.get("Duty")

        final_doc["supplier"] = pw.get("Supplier")

        # Parts line-item summaries (joined)
        line_items = pw.get("LineItems") if isinstance(pw.get("LineItems"), list) else []
        def join_vals(key: str) -> str:
            vals = []
            for it in line_items:
                if isinstance(it, dict):
                    v = it.get(key)
                    if v not in (None, "", []):
                        vals.append(str(v))
            # de-dupe preserving order
            seen = set()
            out = []
            for x in vals:
                if x not in seen:
                    seen.add(x)
                    out.append(x)
            return " | ".join(out)

        final_doc["partsInvoiceSeq"] = join_vals("Invoice_Seq")
        final_doc["partsInvoiceNumber"] = join_vals("Invoice_Number")
        final_doc["partsPartNo"] = join_vals("Part_No")
        final_doc["partsHTS"] = join_vals("HTS_code")
        final_doc["partsDescription"] = join_vals("Description_of_Goods")

        container.upsert_item(final_doc)

        # cleanup uploads
        delete_prefix(uploads, f"{job_id}/")

    except Exception as e:
        container.upsert_item({
            "id": job_id or str(uuid.uuid4()),
            "type": "bundle_result",
            "fileName": file_name,
            "createdAt": created_at,
            "status": "failed",
            "error": str(e),
            "failedAt": utc_now_iso(),
            "sourcePdfBlobPath": pdf_blob_path,
        })
        raise


# =========================
# GET job details (UI)
# =========================

@app.route(route="job/{jobId}/ui", methods=["GET"])
def get_job_ui(req: func.HttpRequest) -> func.HttpResponse:
    job_id = req.route_params.get("jobId")
    if not job_id:
        return func.HttpResponse(json.dumps({"error": "Missing jobId"}), status_code=400, mimetype="application/json")

    container = cosmos_container()
    try:
        doc = container.read_item(item=job_id, partition_key=job_id)
    except CosmosResourceNotFoundError:
        return func.HttpResponse(json.dumps({"id": job_id, "status": "not_found"}), status_code=404, mimetype="application/json")

    status = doc.get("status")
    if status == "processing":
        return func.HttpResponse(json.dumps({"id": job_id, "status": "processing", "createdAt": doc.get("createdAt")}), status_code=200, mimetype="application/json")

    if status == "failed":
        return func.HttpResponse(json.dumps({"id": job_id, "status": "failed", "error": doc.get("error"), "failedAt": doc.get("failedAt")}, default=str), status_code=200, mimetype="application/json")

    # ensure parts worksheet is always clean (defensive)
    if isinstance(doc.get("parts_worksheet"), dict):
        doc["parts_worksheet"] = normalize_parts_worksheet_fields(doc["parts_worksheet"])

    return func.HttpResponse(json.dumps(doc, default=str), status_code=200, mimetype="application/json")


# =========================
# Results listing (pagination + invoice date filter)
# GET /api/results?pageSize=20&token=...&range=last_month&invoiceFrom=YYYY-MM-DD&invoiceTo=YYYY-MM-DD
# =========================

def _get_query_param(req: func.HttpRequest, name: str, default=None):
    v = req.params.get(name)
    return v if v is not None else default

def _parse_int(s, default: int):
    try:
        return int(s)
    except Exception:
        return default

@app.route(route="results", methods=["GET"])
def list_results(req: func.HttpRequest) -> func.HttpResponse:
    container = cosmos_container()

    page_size = _parse_int(_get_query_param(req, "pageSize", str(DEFAULT_PAGE_SIZE)), DEFAULT_PAGE_SIZE)
    page_size = max(1, min(MAX_PAGE_SIZE, page_size))

    token = _get_query_param(req, "token", None)

    invoice_from = _parse_iso_date_only(_get_query_param(req, "invoiceFrom", None))
    invoice_to = _parse_iso_date_only(_get_query_param(req, "invoiceTo", None))
    range_key = _get_query_param(req, "range", None)

    if range_key and range_key.strip().lower() != "all":
        rf, rt = _compute_range(range_key)
        if not invoice_from and not invoice_to:
            invoice_from, invoice_to = rf, rt

    where = ['c.type = "bundle_result"']
    params = []

    # invoice date filters
    if invoice_from:
        where.append("IS_DEFINED(c.cevaInvoiceDate) AND c.cevaInvoiceDate >= @invFrom")
        params.append({"name": "@invFrom", "value": invoice_from})
    if invoice_to:
        where.append("IS_DEFINED(c.cevaInvoiceDate) AND c.cevaInvoiceDate <= @invTo")
        params.append({"name": "@invTo", "value": invoice_to})

    where_clause = " AND ".join(where)

    # IMPORTANT: Return exactly the columns you want to show in the main table
    query = f"""
    SELECT
      c.id,
      c.fileName,
      c.cevaInvoiceDate,
      c.invoiceNumber,
      c.entryNumber,
      c.departureDate,
      c.arrivalDate,
      c.descriptionOfGoods,
      c.totalEnteredValue,
      c.totalOtherFees,
      c.duty,
      c.supplier,
      c.partsInvoiceSeq,
      c.partsInvoiceNumber,
      c.partsPartNo,
      c.partsHTS,
      c.partsDescription
    FROM c
    WHERE {where_clause}
    ORDER BY c.createdAt DESC
    """

    try:
        items_iterable = container.query_items(
            query=query,
            parameters=params,
            enable_cross_partition_query=True,
            max_item_count=page_size,
        )

        # IMPORTANT: by_page does NOT accept max_item_count in your SDK -> only continuation_token
        pager = items_iterable.by_page(continuation_token=token)
        page = next(pager)
        items = list(page)
        next_token = getattr(page, "continuation_token", None)

        return func.HttpResponse(
            json.dumps({"items": items, "continuationToken": next_token}, default=str),
            mimetype="application/json",
            status_code=200,
        )

    except StopIteration:
        return func.HttpResponse(
            json.dumps({"items": [], "continuationToken": None}),
            mimetype="application/json",
            status_code=200,
        )
    except Exception as e:
        return func.HttpResponse(json.dumps({"error": str(e)}), mimetype="application/json", status_code=500)


# =========================
# Excel Export (ALL matching docs)
# GET /api/results/excel?range=...&invoiceFrom=...&invoiceTo=...
# =========================

def _autosize_columns(ws, max_col: int, max_width: int = 60):
    for col_idx in range(1, max_col + 1):
        col_letter = get_column_letter(col_idx)
        max_len = 0
        for cell in ws[col_letter]:
            if cell.value is None:
                continue
            max_len = max(max_len, len(str(cell.value)))
        ws.column_dimensions[col_letter].width = min(max_len + 2, max_width)

@app.route(route="results/excel", methods=["GET"])
def export_results_excel(req: func.HttpRequest) -> func.HttpResponse:
    container = cosmos_container()

    invoice_from = _parse_iso_date_only(_get_query_param(req, "invoiceFrom", None))
    invoice_to = _parse_iso_date_only(_get_query_param(req, "invoiceTo", None))
    range_key = _get_query_param(req, "range", None)

    if range_key and range_key.strip().lower() != "all":
        rf, rt = _compute_range(range_key)
        if not invoice_from and not invoice_to:
            invoice_from, invoice_to = rf, rt

    where = ['c.type = "bundle_result"', 'c.status = "completed"']
    params = []

    if invoice_from:
        where.append("IS_DEFINED(c.cevaInvoiceDate) AND c.cevaInvoiceDate >= @invFrom")
        params.append({"name": "@invFrom", "value": invoice_from})
    if invoice_to:
        where.append("IS_DEFINED(c.cevaInvoiceDate) AND c.cevaInvoiceDate <= @invTo")
        params.append({"name": "@invTo", "value": invoice_to})

    where_clause = " AND ".join(where)

    query = f"""
    SELECT
      c.id,
      c.fileName,
      c.cevaInvoiceDate,
      c.invoiceNumber,
      c.entryNumber,
      c.departureDate,
      c.arrivalDate,
      c.descriptionOfGoods,
      c.totalEnteredValue,
      c.totalOtherFees,
      c.duty,
      c.supplier,
      c.partsInvoiceSeq,
      c.partsInvoiceNumber,
      c.partsPartNo,
      c.partsHTS,
      c.partsDescription,
      c.parts_worksheet
    FROM c
    WHERE {where_clause}
    ORDER BY c.createdAt DESC
    """

    try:
        items_iterable = container.query_items(
            query=query,
            parameters=params,
            enable_cross_partition_query=True,
            max_item_count=200,
        )

        all_docs = []
        for page in items_iterable.by_page():
            all_docs.extend(list(page))

        if not all_docs:
            return func.HttpResponse(
                json.dumps({"error": "No matching documents for export."}),
                mimetype="application/json",
                status_code=400,
            )

        wb = Workbook()

        # Jobs sheet (matches website columns)
        ws_jobs = wb.active
        ws_jobs.title = "Jobs"

        headers = [
            "File Name",
            "CEVA INVOICE DATE",
            "INVOICE NUMBER",
            "ENTRY NUMBER",
            "DEPARTURE DATE",
            "ARRIVAL DATE",
            "DESCRIPTION OF GOODS",
            "TOTAL ENTERED VALUE",
            "TOTAL OTHER FEES",
            "DUTY",
            "SUPPLIER",
            "PARTS Invoice_Seq",
            "PARTS Invoice_Number",
            "PARTS Part_No",
            "PARTS HTS_code",
            "PARTS Description_of_Goods",
        ]
        ws_jobs.append(headers)

        for d in all_docs:
            ws_jobs.append([
                d.get("fileName"),
                d.get("cevaInvoiceDate"),
                d.get("invoiceNumber"),
                d.get("entryNumber"),
                d.get("departureDate"),
                d.get("arrivalDate"),
                d.get("descriptionOfGoods"),
                d.get("totalEnteredValue"),
                d.get("totalOtherFees"),
                d.get("duty"),
                d.get("supplier"),
                d.get("partsInvoiceSeq"),
                d.get("partsInvoiceNumber"),
                d.get("partsPartNo"),
                d.get("partsHTS"),
                d.get("partsDescription"),
            ])

        _autosize_columns(ws_jobs, len(headers))

        # LineItems sheet (one row per line item)
        ws_li = wb.create_sheet("LineItems")
        ws_li.append(["jobId", "fileName", "cevaInvoiceDate", "Invoice_Seq", "Invoice_Number", "Part_No", "HTS_code", "Description_of_Goods"])

        wrote_any = False
        for d in all_docs:
            pw = d.get("parts_worksheet") if isinstance(d.get("parts_worksheet"), dict) else {}
            pw = normalize_parts_worksheet_fields(pw)  # defensive
            li = pw.get("LineItems") if isinstance(pw.get("LineItems"), list) else []
            for it in li:
                if not isinstance(it, dict):
                    continue
                ws_li.append([
                    d.get("id"),
                    d.get("fileName"),
                    d.get("cevaInvoiceDate"),
                    it.get("Invoice_Seq"),
                    it.get("Invoice_Number"),
                    it.get("Part_No"),
                    it.get("HTS_code"),
                    it.get("Description_of_Goods"),
                ])
                wrote_any = True

        if not wrote_any:
            ws_li.append(["(no line items found)"])

        _autosize_columns(ws_li, 8)

        out = io.BytesIO()
        wb.save(out)
        out.seek(0)

        suffix = "all"
        if invoice_from or invoice_to:
            suffix = f"{invoice_from or '...'}_to_{invoice_to or '...'}"
        elif range_key:
            suffix = range_key

        filename = f"bundle_export_{suffix}.xlsx"
        headers_resp = {"Content-Disposition": f'attachment; filename="{filename}"'}

        return func.HttpResponse(
            body=out.getvalue(),
            status_code=200,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers=headers_resp,
        )

    except Exception as e:
        return func.HttpResponse(json.dumps({"error": str(e)}), mimetype="application/json", status_code=500)
