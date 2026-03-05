import azure.functions as func
import json
import os
import uuid
import base64
import io
import zipfile
import concurrent.futures
from datetime import datetime, timezone, timedelta
from typing import Optional, Any, Dict, List, Tuple

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


# ============================================================
# CONFIG
# ============================================================
UPLOADS_CONTAINER       = os.environ.get("UPLOADS_CONTAINER", "uploads")
COSMOS_DATABASE         = os.environ.get("COSMOS_DATABASE", "pdfbundle")
COSMOS_CONTAINER        = os.environ.get("COSMOS_CONTAINER", "results")
CLASSIFIER_ID           = os.environ.get("DI_CLASSIFIER_ID", "cevadocclassmodel")
ACTIVE_BATCH_RECORD_ID  = "active_batch"

TARGET_DOC_TYPES = ["CEVA", "ENTRY SUMMARY", "PARTS WORKSHEET"]

EXTRACTION_MODELS = {
    "CEVA":             "ceva_invoice_model",
    "ENTRY SUMMARY":    "entry-summary-v1",
    "PARTS WORKSHEET":  "partsworksheet_model",
}

DEFAULT_PAGE_SIZE       = 20
MAX_PAGE_SIZE           = 200
CEVA_INVOICE_DATE_FIELD = "INVOICE DATE"


# ============================================================
# TIME HELPERS
# ============================================================
def utc_now() -> datetime:
    return datetime.now(timezone.utc)

def utc_now_iso() -> str:
    return utc_now().isoformat()


# ============================================================
# CLIENT HELPERS
# All clients are created fresh per-call (Azure Functions best practice).
# Heavy calls (DI extractions) reuse a single client instance passed in.
# ============================================================
def blob_service() -> BlobServiceClient:
    return BlobServiceClient.from_connection_string(os.environ["AzureWebJobsStorage"])

def queue_client() -> QueueClient:
    q = QueueClient.from_connection_string(os.environ["AzureWebJobsStorage"], "jobs")
    try:
        q.create_queue()
    except Exception:
        pass
    return q

def di_client() -> DocumentIntelligenceClient:
    return DocumentIntelligenceClient(
        endpoint=os.environ["DI_ENDPOINT"],
        credential=AzureKeyCredential(os.environ["DI_KEY"]),
    )

def cosmos_container():
    client = CosmosClient(os.environ["COSMOS_ENDPOINT"], credential=os.environ["COSMOS_KEY"])
    db = client.get_database_client(COSMOS_DATABASE)
    return db.get_container_client(COSMOS_CONTAINER)


# ============================================================
# SMALL UTILS
# ============================================================
def normalize_doctype(s: Optional[str]) -> str:
    return (s or "").strip().upper()

def safe_slug(s: str) -> str:
    return (s or "").strip().upper().replace(" ", "_")

def _is_pdf_filename(name: str) -> bool:
    return (name or "").lower().endswith(".pdf")

def _is_zip_filename(name: str) -> bool:
    return (name or "").lower().endswith(".zip")

def _parse_int(s: Optional[str], default: int) -> int:
    try:
        return int(s)
    except Exception:
        return default

def _parse_iso_date_only(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    s = s.strip()
    if len(s) >= 10 and s[4] == "-" and s[7] == "-":
        return s[:10]
    return None

def delete_prefix(container_client, prefix: str):
    for blob in container_client.list_blobs(name_starts_with=prefix):
        container_client.delete_blob(blob.name)


# ============================================================
# DI VALUE NORMALIZATION
# ============================================================
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
    d = getattr(obj, "__dict__", None)
    if isinstance(d, dict) and d:
        return {k: _jsonable(v) for k, v in d.items()}
    return str(obj)


def _normalize_di_wrapped_value(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, (str, int, float, bool)):
        return v
    if isinstance(v, list):
        return [_normalize_di_wrapped_value(x) for x in v]
    if isinstance(v, dict):
        for key in ("valueString", "valueNumber", "valueInteger", "valueBoolean",
                    "valueDate", "valueTime", "valuePhoneNumber", "valueSelectionMark"):
            if key in v:
                return v[key]
        if "valueObject" in v and isinstance(v["valueObject"], dict):
            return {k: _normalize_di_wrapped_value(val) for k, val in v["valueObject"].items()}
        if "valueArray" in v and isinstance(v["valueArray"], list):
            return [_normalize_di_wrapped_value(item) for item in v["valueArray"]]
        drop = {"boundingRegions", "polygon", "spans"}
        return {k: _normalize_di_wrapped_value(val) for k, val in v.items()
                if k not in drop and k != "content"}
    return _normalize_di_wrapped_value(_jsonable(v))


def _field_value_simple(field):
    if field is None:
        return None
    value = getattr(field, "value", None)
    if value is not None:
        return _normalize_di_wrapped_value(_jsonable(value))
    content = getattr(field, "content", None)
    if content is not None:
        return content
    return _normalize_di_wrapped_value(_jsonable(field))


def simplify_analyze_result(result) -> dict:
    docs = getattr(result, "documents", None) or []
    if not docs:
        return {"fields": {}, "hasData": False}
    fields = getattr(docs[0], "fields", None) or {}
    simple = {str(k): _field_value_simple(v) for k, v in fields.items()}
    has_data = any(v not in (None, "", [], {}) for v in simple.values())
    return {"fields": simple, "hasData": has_data}


# ============================================================
# PARTS WORKSHEET — LINE ITEM NORMALIZATION
# ============================================================
def _unwrap_line_item_field(fv: Any) -> Any:
    """Fully unwrap a single DI field value to a plain scalar or structure."""
    if fv is None:
        return None
    if isinstance(fv, (str, int, float, bool)):
        return fv
    if isinstance(fv, list):
        return [_unwrap_line_item_field(i) for i in fv]
    if isinstance(fv, dict):
        for key in ("valueString", "valueNumber", "valueInteger", "valueDate",
                    "valueTime", "valueBoolean", "valuePhoneNumber", "valueSelectionMark"):
            if key in fv:
                val = fv[key]
                return _unwrap_line_item_field(val) if isinstance(val, dict) else val
        if "valueObject" in fv and isinstance(fv["valueObject"], dict):
            return {k: _unwrap_line_item_field(v) for k, v in fv["valueObject"].items()}
        if "valueArray" in fv and isinstance(fv["valueArray"], list):
            return [_unwrap_line_item_field(i) for i in fv["valueArray"]]
        # content-only fallback — DI omits valueString on low-confidence / rotated pages
        if "content" in fv:
            return str(fv["content"])
        # No extractable DI value — return None to avoid leaking metadata dicts
        return None
    return str(fv)


def _dedup_spacejoined(s: str) -> str:
    """
    DI sometimes returns 'VALUE VALUE' when a field spans multiple OCR regions.
    Deduplicates exact half-repetitions only — never mangles legitimate strings.
    e.g. 'COOLER CORE COOLER CORE' => 'COOLER CORE'
         '4965482 4965483'         => '4965482, 4965483'  (distinct => join)
    """
    s = s.strip()
    if not s:
        return s
    tokens = s.split(" ")
    n = len(tokens)
    unique = list(dict.fromkeys(tokens))
    if len(unique) == 1:
        return unique[0]
    if n >= 2 and n % 2 == 0:
        half = n // 2
        first_half  = " ".join(tokens[:half])
        second_half = " ".join(tokens[half:])
        if first_half == second_half:
            return first_half
    return s


def _fix_parts_worksheet_line_items(pw_fields: dict) -> dict:
    """
    Ensures parts_worksheet.LineItems is a clean plain list of dicts.
    Handles all storage cases: new clean list, old DI wrapper, missing.
    Applies _dedup_spacejoined to all string field values.
    Safe to call on both freshly extracted and old Cosmos documents.
    """
    if not isinstance(pw_fields, dict):
        return pw_fields

    line_items = pw_fields.get("LineItems")
    raw_items: List[Any] = []

    if isinstance(line_items, list):
        raw_items = line_items
    elif isinstance(line_items, dict) and "valueArray" in line_items:
        for item in (line_items.get("valueArray") or []):
            if not isinstance(item, dict):
                continue
            vo = item.get("valueObject", {})
            raw_items.append(vo if isinstance(vo, dict) else item)
    else:
        pw_fields["LineItems"] = []
        return pw_fields

    clean_items = []
    for item in raw_items:
        if not isinstance(item, dict):
            continue
        row = {}
        for field_name, field_val in item.items():
            unwrapped = _unwrap_line_item_field(field_val)
            if isinstance(unwrapped, str):
                unwrapped = _dedup_spacejoined(unwrapped)
            row[field_name] = unwrapped
        clean_items.append(row)

    pw_fields["LineItems"] = clean_items
    return pw_fields


# ============================================================
# PDF HELPERS
# ============================================================
def split_pdf_to_single_pages(pdf_bytes: bytes) -> List[Dict[str, Any]]:
    reader = PdfReader(io.BytesIO(pdf_bytes))
    out = []
    for i, page in enumerate(reader.pages):
        w = PdfWriter()
        w.add_page(page)
        buf = io.BytesIO()
        w.write(buf)
        out.append({"pageNumber": i + 1, "pdfBytes": buf.getvalue()})
    return out


def extract_pages(reader: PdfReader, page_numbers: List[int]) -> bytes:
    """
    Extract specific pages from an already-parsed PdfReader.
    Caller should parse PDF bytes once and reuse the reader.
    """
    writer = PdfWriter()
    for p in page_numbers:
        writer.add_page(reader.pages[p - 1])
    buf = io.BytesIO()
    writer.write(buf)
    return buf.getvalue()


# ============================================================
# INVOICE DATE PARSING / NORMALIZATION
# ============================================================
def _try_parse_date_to_yyyy_mm_dd(value: Any) -> Optional[str]:
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    # Already ISO
    if len(s) >= 10 and s[4] == "-" and s[7] == "-":
        return s[:10]
    s2 = s.replace(".", "/").replace("-", "/")
    parts = [p.strip() for p in s2.split("/") if p.strip()]
    # YYYY/MM/DD
    if len(parts) == 3 and len(parts[0]) == 4 and parts[0].isdigit():
        y, m, d = parts[0], parts[1].zfill(2), parts[2].zfill(2)
        if m.isdigit() and d.isdigit():
            return f"{y}-{m}-{d}"
    # DD/MM/YYYY or MM/DD/YYYY
    if len(parts) == 3 and len(parts[2]) == 4 and parts[2].isdigit():
        a, b, y = parts[0], parts[1], parts[2]
        if a.isdigit() and b.isdigit():
            da, db = int(a), int(b)
            d, m = (da, db) if da > 12 else (db, da) if db > 12 else (da, db)
            return f"{y}-{str(m).zfill(2)}-{str(d).zfill(2)}"
    for fmt in ("%d %b %Y", "%d %B %Y"):
        try:
            t = s.replace("-", " ").replace("/", " ").strip()
            return datetime.strptime(t, fmt).strftime("%Y-%m-%d")
        except Exception:
            pass
    return None


def _extract_ceva_invoice_date(doc: dict) -> Optional[str]:
    ceva = doc.get("ceva") if isinstance(doc.get("ceva"), dict) else {}
    if CEVA_INVOICE_DATE_FIELD in ceva:
        parsed = _try_parse_date_to_yyyy_mm_dd(ceva.get(CEVA_INVOICE_DATE_FIELD))
        if parsed:
            return parsed
    for k, v in ceva.items():
        lk = str(k).strip().lower()
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
    if rk in ("last_week", "last_7_days"):
        start = today - timedelta(days=7)
        return (start.strftime("%Y-%m-%d"), today.strftime("%Y-%m-%d"))
    if rk == "last_30_days":
        start = today - timedelta(days=30)
        return (start.strftime("%Y-%m-%d"), today.strftime("%Y-%m-%d"))
    if rk == "last_month":
        first_this_month  = today.replace(day=1)
        last_prev_month   = first_this_month - timedelta(days=1)
        first_prev_month  = last_prev_month.replace(day=1)
        return (first_prev_month.strftime("%Y-%m-%d"), last_prev_month.strftime("%Y-%m-%d"))
    return (None, None)


# ============================================================
# SUMMARY COLUMNS
# ============================================================
SUMMARY_COLS = [
    ("fileName",                   "File Name"),
    ("cevaInvoiceDate",            "CEVA INVOICE DATE"),
    ("invoiceNumber",              "INVOICE NUMBER"),
    ("entryNumber",                "ENTRY NUMBER"),
    ("departureDate",              "DEPARTURE DATE"),
    ("arrivalDate",                "ARRIVAL DATE"),
    ("descriptionOfGoods",         "DESCRIPTION OF GOODS"),
    ("totalEnteredValue",          "TOTAL ENTERED VALUE"),
    ("totalOtherFees",             "TOTAL OTHER FEES"),
    ("duty",                       "DUTY"),
    ("supplier",                   "SUPPLIER"),
    ("parts_Invoice_Seq",          "PARTS Invoice_Seq"),
    ("parts_Invoice_Number",       "PARTS Invoice_Number"),
    ("parts_Part_No",              "PARTS Part_No"),
    ("parts_HTS_code",             "PARTS HTS_code"),
    ("parts_Description_of_Goods", "PARTS Description_of_Goods"),
]

def _safe_to_cell(v: Any) -> Any:
    if v is None:
        return ""
    if isinstance(v, (str, int, float, bool)):
        return v
    try:
        return json.dumps(v, ensure_ascii=False)
    except Exception:
        return str(v)


def _join_all_line_item_values(line_items: List[dict], field: str) -> str:
    """
    Collect `field` from every line item and join distinct non-empty values
    with ', '. Shows all parts in summary columns, not just the first row.
    """
    seen = []
    for item in (line_items or []):
        v = item.get(field)
        if v is None:
            continue
        s = str(v).strip()
        if s and s not in seen:
            seen.append(s)
    return ", ".join(seen)


# ============================================================
# BATCH MANAGEMENT — clear stale queue jobs
# ============================================================
def _set_active_batch(batch_id: str) -> None:
    """Write the current batch ID to Cosmos so workers can check staleness."""
    cosmos_container().upsert_item({
        "id":      ACTIVE_BATCH_RECORD_ID,
        "type":    ACTIVE_BATCH_RECORD_ID,
        "batchId": batch_id,
    })


def _get_active_batch_id() -> Optional[str]:
    try:
        doc = cosmos_container().read_item(
            item=ACTIVE_BATCH_RECORD_ID,
            partition_key=ACTIVE_BATCH_RECORD_ID,
        )
        return doc.get("batchId")
    except CosmosResourceNotFoundError:
        return None
    except Exception:
        return None


def _clear_queue_safe(qc: QueueClient) -> None:
    """
    Delete all pending (visible) messages from the queue.
    Messages already dequeued by a worker are invisible and won't be cleared here —
    those are handled by the batchId staleness check in job_worker.
    """
    try:
        qc.clear_messages()
    except Exception:
        pass  # Queue may already be empty — safe to ignore


# ============================================================
# HTTP: UPLOAD (PDF + ZIP)
# ============================================================
@app.route(route="upload", methods=["POST"])
def upload(req: func.HttpRequest) -> func.HttpResponse:
    files = req.files.getlist("files")
    if not files:
        return func.HttpResponse(
            json.dumps({"error": "Upload files via multipart/form-data field 'files'"}),
            status_code=400, mimetype="application/json",
        )

    bsc = blob_service()
    uploads = bsc.get_container_client(UPLOADS_CONTAINER)
    try:
        uploads.create_container()
    except Exception:
        pass

    qc = queue_client()

    # ----------------------------------------------------------
    # ✅ STEP 1: Clear pending queue messages from previous runs
    # ----------------------------------------------------------
    _clear_queue_safe(qc)

    # ----------------------------------------------------------
    # ✅ STEP 2: Register new batch ID in Cosmos.
    # Workers dequeued before the clear will check this and self-discard.
    # ----------------------------------------------------------
    batch_id = str(uuid.uuid4())
    _set_active_batch(batch_id)

    jobs = []

    def enqueue_one_pdf(pdf_bytes: bytes, filename: str) -> dict:
        job_id    = str(uuid.uuid4())
        blob_path = f"{job_id}/original/{filename}"
        uploads.upload_blob(blob_path, pdf_bytes, overwrite=True)
        qc.send_message(json.dumps({
            "jobId":       job_id,
            "pdfBlobPath": blob_path,
            "fileName":    filename,
            "batchId":     batch_id,   # ✅ stamp every message with current batch
        }))
        return {
            "jobId":    job_id,
            "fileName": filename,
            "blobPath": blob_path,
            "bytes":    len(pdf_bytes),
        }

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
                    json.dumps({"error": f"Unsupported file type: {filename}. Upload PDF or ZIP of PDFs."}),
                    status_code=400, mimetype="application/json",
                )

        resp = {"batchId": batch_id, "jobs": jobs}
        if len(jobs) == 1:
            resp["jobId"] = jobs[0]["jobId"]
        return func.HttpResponse(json.dumps(resp), status_code=200, mimetype="application/json")

    except zipfile.BadZipFile:
        return func.HttpResponse(
            json.dumps({"error": "Invalid ZIP file."}),
            status_code=400, mimetype="application/json",
        )
    except Exception as e:
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            status_code=500, mimetype="application/json",
        )


# ============================================================
# QUEUE WORKER
# ============================================================
@app.function_name(name="job_worker")
@app.queue_trigger(arg_name="msg", queue_name="jobs", connection="AzureWebJobsStorage")
def job_worker(msg: func.QueueMessage) -> None:
    payload       = json.loads(msg.get_body().decode("utf-8"))
    job_id        = payload.get("jobId")
    pdf_blob_path = payload.get("pdfBlobPath")
    file_name     = payload.get("fileName")
    msg_batch_id  = payload.get("batchId")

    uploads_client = blob_service().get_container_client(UPLOADS_CONTAINER)
    container      = cosmos_container()
    created_at     = utc_now_iso()

    # ----------------------------------------------------------
    # ✅ STALENESS CHECK — discard jobs from superseded batches.
    # Covers the race condition where clear_messages() was called
    # but this message was already invisible (dequeued by runtime).
    # ----------------------------------------------------------
    active_batch_id = _get_active_batch_id()
    if msg_batch_id and active_batch_id and msg_batch_id != active_batch_id:
        # Clean up orphaned blob and exit silently
        try:
            uploads_client.delete_blob(pdf_blob_path)
        except Exception:
            pass
        return

    try:
        container.upsert_item({
            "id":        job_id,
            "type":      "bundle_result",
            "fileName":  file_name,
            "createdAt": created_at,
            "status":    "processing",
        })

        pdf_bytes = uploads_client.get_blob_client(pdf_blob_path).download_blob().readall()

        # Create a single DI client and PDF reader — reused across all calls
        client = di_client()
        reader = PdfReader(io.BytesIO(pdf_bytes))

        # ------------------------------------------------------
        # 1️⃣ Classify entire PDF once (sequential — required first)
        # ------------------------------------------------------
        poller               = client.begin_classify_document(
            CLASSIFIER_ID,
            body=pdf_bytes,
            content_type="application/pdf",
        )
        classification_result = poller.result()

        best_docs: Dict[str, Dict] = {}
        for doc in classification_result.documents:
            doc_type   = normalize_doctype(doc.doc_type)
            confidence = doc.confidence
            pages      = [r.page_number for r in doc.bounding_regions]
            if doc_type in TARGET_DOC_TYPES:
                existing = best_docs.get(doc_type)
                if not existing or confidence > existing["confidence"]:
                    best_docs[doc_type] = {"confidence": confidence, "pages": pages}

        # ------------------------------------------------------
        # 2️⃣ Run all extraction models IN PARALLEL
        # ------------------------------------------------------
        def extract_one(doc_type: str, info: dict) -> Optional[dict]:
            """
            Runs one extraction model and returns a result dict.
            Executed concurrently in a thread pool — each call gets
            its own DI client instance to avoid shared-state issues.
            """
            model_id = EXTRACTION_MODELS.get(doc_type)
            if not model_id:
                return None

            extracted_pdf = extract_pages(reader, info["pages"])

            # Each thread creates its own DI client (not thread-safe to share)
            thread_client = di_client()
            poller        = thread_client.begin_analyze_document(
                model_id,
                body=extracted_pdf,
                content_type="application/pdf",
            )
            analyze_result = poller.result()
            simplified     = simplify_analyze_result(analyze_result)
            fields         = simplified.get("fields", {})

            key = safe_slug(doc_type).lower()
            if key == "parts_worksheet":
                fields = _fix_parts_worksheet_line_items(fields)

            return {
                "doc_type":   doc_type,
                "key":        key,
                "model_id":   model_id,
                "pages":      info["pages"],
                "confidence": info["confidence"],
                "fields":     fields,
            }

        merged       = {}
        picked_pages = {}
        confidence   = {}
        extracted_docs = []

        # ThreadPoolExecutor — ideal for I/O-bound DI HTTP calls
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(best_docs) or 1) as executor:
            future_map = {
                executor.submit(extract_one, doc_type, info): doc_type
                for doc_type, info in best_docs.items()
            }
            for future in concurrent.futures.as_completed(future_map):
                result = future.result()
                if result is None:
                    continue
                merged[result["key"]]              = result["fields"]
                picked_pages[result["doc_type"]]   = result["pages"]
                confidence[result["doc_type"]]     = result["confidence"]
                extracted_docs.append({
                    "docType":    result["doc_type"],
                    "modelId":    result["model_id"],
                    "pages":      result["pages"],
                    "fieldCount": len(result["fields"]),
                })

        # ------------------------------------------------------
        # 3️⃣ Save completed result to Cosmos
        # ------------------------------------------------------
        final_doc = {
            "id":                job_id,
            "type":              "bundle_result",
            "fileName":          file_name,
            "createdAt":         created_at,
            "completedAt":       utc_now_iso(),
            "status":            "completed",
            "sourcePdfBlobPath": pdf_blob_path,
            "pickedPages":       picked_pages,
            "confidence":        confidence,
            "extractedDocs":     extracted_docs,
            **merged,
        }
        final_doc["cevaInvoiceDate"] = _extract_ceva_invoice_date(final_doc)

        container.upsert_item(final_doc)

        # Clean up uploaded blob after successful processing
        delete_prefix(uploads_client, f"{job_id}/")

    except Exception as e:
        # Record failure — blob intentionally left for debugging
        container.upsert_item({
            "id":                job_id or str(uuid.uuid4()),
            "type":              "bundle_result",
            "fileName":          file_name,
            "createdAt":         created_at,
            "status":            "failed",
            "error":             str(e),
            "failedAt":          utc_now_iso(),
            "sourcePdfBlobPath": pdf_blob_path,
        })
        raise


# ============================================================
# HTTP: JOB DETAIL (UI-READY)
# ============================================================
@app.route(route="job/{jobId}/ui", methods=["GET"])
def get_job_ui(req: func.HttpRequest) -> func.HttpResponse:
    job_id = req.route_params.get("jobId")
    if not job_id:
        return func.HttpResponse(
            json.dumps({"error": "Missing jobId"}),
            status_code=400, mimetype="application/json",
        )

    container = cosmos_container()
    try:
        doc = container.read_item(item=job_id, partition_key=job_id)
    except CosmosResourceNotFoundError:
        return func.HttpResponse(
            json.dumps({"id": job_id, "status": "not_found"}),
            status_code=404, mimetype="application/json",
        )

    status = doc.get("status")

    if status in ("processing", "failed"):
        out = {"id": job_id, "status": status}
        if status == "processing":
            out["createdAt"] = doc.get("createdAt")
        if status == "failed":
            out["error"]    = doc.get("error")
            out["failedAt"] = doc.get("failedAt")
        return func.HttpResponse(
            json.dumps(out, default=str),
            status_code=200, mimetype="application/json",
        )

    def strip_internal(d: dict) -> dict:
        return {k: v for k, v in d.items() if not k.startswith("_")} if isinstance(d, dict) else d

    # Fix wrapped LineItems on old Cosmos docs — no reprocessing required
    pw_raw   = strip_internal(doc.get("parts_worksheet") or {})
    pw_fixed = _fix_parts_worksheet_line_items(pw_raw)

    out = {
        "id":              doc.get("id"),
        "fileName":        doc.get("fileName"),
        "status":          doc.get("status"),
        "createdAt":       doc.get("createdAt"),
        "completedAt":     doc.get("completedAt"),
        "cevaInvoiceDate": doc.get("cevaInvoiceDate"),
        "ceva":            strip_internal(doc.get("ceva") or {}),
        "entry_summary":   strip_internal(doc.get("entry_summary") or {}),
        "parts_worksheet": pw_fixed,
    }
    return func.HttpResponse(
        json.dumps(out, default=str),
        status_code=200, mimetype="application/json",
    )


# ============================================================
# COSMOS QUERY BUILDER (shared by list + excel)
# ============================================================
def _build_filters_from_req(req: func.HttpRequest):
    q           = req.params.get("q")
    range_key   = req.params.get("range")
    invoice_from = _parse_iso_date_only(req.params.get("invoiceFrom"))
    invoice_to   = _parse_iso_date_only(req.params.get("invoiceTo"))

    if range_key and not (invoice_from or invoice_to) and range_key.strip().lower() != "all":
        rf, rt = _compute_range(range_key)
        invoice_from, invoice_to = rf, rt

    where  = ['c.type = "bundle_result"', 'c.status = "completed"']
    params = []

    if q:
        where.append("CONTAINS(LOWER(c.fileName), LOWER(@q))")
        params.append({"name": "@q", "value": q})
    if invoice_from:
        where.append("IS_DEFINED(c.cevaInvoiceDate) AND c.cevaInvoiceDate >= @invFrom")
        params.append({"name": "@invFrom", "value": invoice_from})
    if invoice_to:
        where.append("IS_DEFINED(c.cevaInvoiceDate) AND c.cevaInvoiceDate <= @invTo")
        params.append({"name": "@invTo", "value": invoice_to})

    return " AND ".join(where), params


# ============================================================
# HTTP: RESULTS LIST (PAGINATED)
# Fetches full LineItems from Cosmos, post-processes server-side:
#   - fixes wrapped LineItems (old docs)
#   - builds comma-joined summary columns across ALL line items
# ============================================================
@app.route(route="results", methods=["GET"])
def list_results(req: func.HttpRequest) -> func.HttpResponse:
    container = cosmos_container()

    page_size = _parse_int(req.params.get("pageSize"), DEFAULT_PAGE_SIZE)
    page_size = max(1, min(MAX_PAGE_SIZE, page_size))
    token     = req.params.get("token")

    where_clause, params = _build_filters_from_req(req)

    query = f"""
    SELECT
      c.id,
      c.fileName,
      c.cevaInvoiceDate,
      c.ceva["INVOICE NUMBER"]      AS invoiceNumber,
      c.ceva["ENTRY NUMBER"]        AS entryNumber,
      c.ceva["DEPARTURE DATE"]      AS departureDate,
      c.ceva["ARRIVAL DATE"]        AS arrivalDate,
      c.ceva["DESCRIPTION OF GOODS"] AS descriptionOfGoods,
      c.entry_summary.Total_Entered_Value AS totalEnteredValue,
      c.entry_summary.Total_Other_Fees    AS totalOtherFees,
      c.entry_summary.Duty                AS duty,
      c.parts_worksheet.Supplier          AS supplier,
      c.parts_worksheet.LineItems         AS lineItems
    FROM c
    WHERE {where_clause}
    ORDER BY c.cevaInvoiceDate DESC
    """

    try:
        items_iterable = container.query_items(
            query=query,
            parameters=params,
            enable_cross_partition_query=True,
            max_item_count=page_size,
        )
        page_iter  = items_iterable.by_page(continuation_token=token)
        page       = next(page_iter)
        raw_items  = list(page)
        next_token = getattr(page, "continuation_token", None)

        items = []
        for item in raw_items:
            li_raw     = item.pop("lineItems", None) or []
            pw_shell   = _fix_parts_worksheet_line_items({"LineItems": li_raw})
            line_items = pw_shell.get("LineItems") or []

            item["parts_Invoice_Seq"]           = _join_all_line_item_values(line_items, "Invoice_Seq")
            item["parts_Invoice_Number"]        = _join_all_line_item_values(line_items, "Invoice_Number")
            item["parts_Part_No"]               = _join_all_line_item_values(line_items, "Part_No")
            item["parts_HTS_code"]              = _join_all_line_item_values(line_items, "HTS_code")
            item["parts_Description_of_Goods"]  = _join_all_line_item_values(line_items, "Description_of_Goods")
            items.append(item)

        return func.HttpResponse(
            json.dumps({"items": items, "continuationToken": next_token}, default=str),
            mimetype="application/json", status_code=200,
        )

    except StopIteration:
        return func.HttpResponse(
            json.dumps({"items": [], "continuationToken": None}),
            mimetype="application/json", status_code=200,
        )
    except Exception as e:
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            mimetype="application/json", status_code=500,
        )


# ============================================================
# EXCEL EXPORT
# Summary sheet : all parts joined with ", "
# LineItems sheet: one row per line item (full granularity)
# ============================================================
def _autosize_columns(ws, max_col: int, max_width: int = 60):
    for col_idx in range(1, max_col + 1):
        col_letter = get_column_letter(col_idx)
        max_len = max(
            (len(str(cell.value)) for cell in ws[col_letter] if cell.value is not None),
            default=0,
        )
        ws.column_dimensions[col_letter].width = min(max_len + 2, max_width)


@app.route(route="results/excel", methods=["GET"])
def export_results_excel(req: func.HttpRequest) -> func.HttpResponse:
    container = cosmos_container()
    where_clause, params = _build_filters_from_req(req)

    query = f"""
    SELECT
      c.id,
      c.fileName,
      c.cevaInvoiceDate,
      c.ceva,
      c.entry_summary,
      c.parts_worksheet
    FROM c
    WHERE {where_clause}
    ORDER BY c.cevaInvoiceDate DESC
    """

    try:
        items_iterable = container.query_items(
            query=query,
            parameters=params,
            enable_cross_partition_query=True,
            max_item_count=200,
        )
        all_docs = [doc for page in items_iterable.by_page() for doc in page]

        wb        = Workbook()
        ws        = wb.active
        ws.title  = "Summary"
        ws.append([label for _, label in SUMMARY_COLS])

        ws_li = wb.create_sheet("LineItems")
        ws_li.append([
            "File Name", "CEVA INVOICE DATE",
            "Invoice_Seq", "Invoice_Number", "Part_No",
            "HTS_code", "Description_of_Goods",
        ])

        for d in all_docs:
            ceva = d.get("ceva")          if isinstance(d.get("ceva"),          dict) else {}
            es   = d.get("entry_summary") if isinstance(d.get("entry_summary"), dict) else {}
            pw   = d.get("parts_worksheet") if isinstance(d.get("parts_worksheet"), dict) else {}

            pw         = _fix_parts_worksheet_line_items(dict(pw))
            line_items = pw.get("LineItems") or []

            row = {
                "fileName":                    d.get("fileName"),
                "cevaInvoiceDate":             d.get("cevaInvoiceDate"),
                "invoiceNumber":               ceva.get("INVOICE NUMBER"),
                "entryNumber":                 ceva.get("ENTRY NUMBER"),
                "departureDate":               ceva.get("DEPARTURE DATE"),
                "arrivalDate":                 ceva.get("ARRIVAL DATE"),
                "descriptionOfGoods":          ceva.get("DESCRIPTION OF GOODS"),
                "totalEnteredValue":           es.get("Total_Entered_Value"),
                "totalOtherFees":              es.get("Total_Other_Fees"),
                "duty":                        es.get("Duty"),
                "supplier":                    pw.get("Supplier"),
                "parts_Invoice_Seq":           _join_all_line_item_values(line_items, "Invoice_Seq"),
                "parts_Invoice_Number":        _join_all_line_item_values(line_items, "Invoice_Number"),
                "parts_Part_No":               _join_all_line_item_values(line_items, "Part_No"),
                "parts_HTS_code":              _join_all_line_item_values(line_items, "HTS_code"),
                "parts_Description_of_Goods":  _join_all_line_item_values(line_items, "Description_of_Goods"),
            }
            ws.append([_safe_to_cell(row.get(key)) for key, _ in SUMMARY_COLS])

            for item in line_items:
                if not isinstance(item, dict):
                    continue
                ws_li.append([
                    _safe_to_cell(d.get("fileName")),
                    _safe_to_cell(d.get("cevaInvoiceDate")),
                    _safe_to_cell(item.get("Invoice_Seq")),
                    _safe_to_cell(item.get("Invoice_Number")),
                    _safe_to_cell(item.get("Part_No")),
                    _safe_to_cell(item.get("HTS_code")),
                    _safe_to_cell(item.get("Description_of_Goods")),
                ])

        _autosize_columns(ws,    len(SUMMARY_COLS))
        _autosize_columns(ws_li, 7)

        out = io.BytesIO()
        wb.save(out)
        out.seek(0)

        range_key    = (req.params.get("range") or "all").strip()
        invoice_from = _parse_iso_date_only(req.params.get("invoiceFrom"))
        invoice_to   = _parse_iso_date_only(req.params.get("invoiceTo"))
        suffix       = range_key.lower() if range_key else "all"
        if invoice_from or invoice_to:
            suffix = f"{invoice_from or '...'}_to_{invoice_to or '...'}"

        filename = f"bundle_export_{suffix}.xlsx"
        return func.HttpResponse(
            body=out.getvalue(),
            status_code=200,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )

    except Exception as e:
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            mimetype="application/json", status_code=500,
        )
