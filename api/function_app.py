import azure.functions as func
import json
import os
import uuid
import base64
import io
import zipfile
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

# -------------------------
# CONFIG
# -------------------------
UPLOADS_CONTAINER = os.environ.get("UPLOADS_CONTAINER", "uploads")

COSMOS_DATABASE = os.environ.get("COSMOS_DATABASE", "pdfbundle")
COSMOS_CONTAINER = os.environ.get("COSMOS_CONTAINER", "results")

CLASSIFIER_ID = os.environ.get("DI_CLASSIFIER_ID", "cevadocclassifier")

TARGET_DOC_TYPES = ["CEVA", "ENTRY SUMMARY", "PARTS WORKSHEET"]

EXTRACTION_MODELS = {
    "CEVA": "ceva_invoice_model",
    "ENTRY SUMMARY": "entry-summary-v1",
    "PARTS WORKSHEET": "partsworksheet_model",
}

ROTATION_FALLBACK_DEGREES = -90

DEFAULT_PAGE_SIZE = 20
MAX_PAGE_SIZE = 200

CEVA_INVOICE_DATE_FIELD = "INVOICE DATE"

# -------------------------
# TIME HELPERS
# -------------------------
def utc_now() -> datetime:
    return datetime.now(timezone.utc)

def utc_now_iso() -> str:
    return utc_now().isoformat()


# -------------------------
# CLIENT HELPERS
# -------------------------
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


# -------------------------
# SMALL UTILS
# -------------------------
def normalize_doctype(s: Optional[str]) -> str:
    return (s or "").strip().upper()

def safe_slug(s: str) -> str:
    return (s or "").strip().upper().replace(" ", "_")

def _is_pdf_filename(name: str) -> bool:
    return (name or "").lower().endswith(".pdf")

def _is_zip_filename(name: str) -> bool:
    return (name or "").lower().endswith(".zip")

def delete_prefix(container_client, prefix: str):
    for blob in container_client.list_blobs(name_starts_with=prefix):
        container_client.delete_blob(blob.name)

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


# -------------------------
# DI VALUE NORMALIZATION
# -------------------------
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
        # FIX: was `for v in d.items()` (bug — iterated tuples, ignored keys)
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
        if "valueString" in v:
            return v.get("valueString")
        if "valueNumber" in v:
            return v.get("valueNumber")
        if "valueInteger" in v:
            return v.get("valueInteger")
        if "valueBoolean" in v:
            return v.get("valueBoolean")
        if "valueDate" in v:
            return v.get("valueDate")
        if "valueTime" in v:
            return v.get("valueTime")
        if "valuePhoneNumber" in v:
            return v.get("valuePhoneNumber")
        if "valueSelectionMark" in v:
            return v.get("valueSelectionMark")
        if "valueObject" in v and isinstance(v["valueObject"], dict):
            return {k: _normalize_di_wrapped_value(val) for k, val in v["valueObject"].items()}
        if "valueArray" in v and isinstance(v["valueArray"], list):
            return [_normalize_di_wrapped_value(item) for item in v["valueArray"]]
        drop = {"boundingRegions", "polygon", "spans"}
        clean = {}
        for k, val in v.items():
            if k in drop or k == "content":
                continue
            clean[k] = _normalize_di_wrapped_value(val)
        return clean
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
# PARTS WORKSHEET LINE ITEMS — NORMALIZATION
#
# _unwrap_line_item_field handles ALL DI field value forms:
#   1. Standard typed:   {"type":"string","valueString":"SEQ 1",...}  => "SEQ 1"
#   2. Content-only:     {"type":"string","content":"SEQ 1"}          => "SEQ 1"
#      (DI omits valueString on low-confidence / rotated page fields)
#   3. Nested objects / arrays
#   4. Already plain str/int/float
#   5. Absolute fallback: str(v) — prevents [object Object] in JS
#
# _fix_parts_worksheet_line_items handles LineItems in all storage forms:
#   Case A — Clean list (new uploads after fix)
#   Case B — DI wrapper: {"type":"array","valueArray":[{"type":"object","valueObject":{...}}]}
#   Case C — Missing / None
#
# _dedup_spacejoined handles fields where DI concatenates repeated OCR spans:
#   "4965482 4965482"            => "4965482"
#   "COOLER CORE COOLER CORE"   => "COOLER CORE"
#   "4965482 4965483"           => "4965482, 4965483"  (distinct => join with ", ")
# -------------------------

def _unwrap_line_item_field(fv: Any) -> Any:
    """Fully unwrap a single DI field value to a plain scalar or structure."""
    if fv is None:
        return None
    if isinstance(fv, (str, int, float, bool)):
        return fv
    if isinstance(fv, list):
        return [_unwrap_line_item_field(i) for i in fv]
    if isinstance(fv, dict):
        # Typed scalar wrappers — priority order
        for key in ("valueString", "valueNumber", "valueInteger", "valueDate",
                    "valueTime", "valueBoolean", "valuePhoneNumber", "valueSelectionMark"):
            if key in fv:
                val = fv[key]
                return _unwrap_line_item_field(val) if isinstance(val, dict) else val

        # Nested object wrapper
        if "valueObject" in fv and isinstance(fv["valueObject"], dict):
            return {k: _unwrap_line_item_field(v) for k, v in fv["valueObject"].items()}

        # Array wrapper
        if "valueArray" in fv and isinstance(fv["valueArray"], list):
            return [_unwrap_line_item_field(i) for i in fv["valueArray"]]

        # CRITICAL: content-only fallback — DI sometimes returns a field with
        # `content` but NO valueString (low-confidence / rotated pages).
        # Without this branch, the whole dict leaks through and JS shows [object Object].
        if "content" in fv:
            return str(fv["content"])

        # Plain dict with no DI keys — recursively clean values
        # return {k: _unwrap_line_item_field(v) for k, v in fv.items()}
    # Plain dict with no extractable DI value — return None rather than
        # leaking metadata dict (type/confidence only, no actual text)
        return None

    # Absolute last resort — ensures JS never sees a raw object
    return str(fv)


def _dedup_spacejoined(s: str) -> str:
    """
    DI sometimes returns a single string where the same value appears twice,
    space-joined, when it spans multiple OCR regions on the page.
    e.g. "4965482 4965482" => "4965482"
         "COOLER CORE COOLER CORE" => "COOLER CORE"
         "4965482 4965483" => "4965482, 4965483"  (distinct values joined)

    Strategy: only deduplicate when we can confirm EXACT repetition (half-split).
    Does not mangle legitimate multi-word strings like "FUEL COMPONENT OF DIESEL ENGINE".
    """
    s = s.strip()
    if not s:
        return s

    tokens = s.split(" ")
    n = len(tokens)

    # All tokens identical (e.g. "SEQ SEQ")
    unique = list(dict.fromkeys(tokens))
    if len(unique) == 1:
        return unique[0]

    # Even-length exact half-repetition (e.g. "COOLER CORE COOLER CORE")
    if n >= 2 and n % 2 == 0:
        half = n // 2
        first_half = " ".join(tokens[:half])
        second_half = " ".join(tokens[half:])
        if first_half == second_half:
            return first_half

    return s


def _fix_parts_worksheet_line_items(pw_fields: dict) -> dict:
    """
    Ensures parts_worksheet.LineItems is a clean plain list of dicts.
    Handles all storage cases (new clean list, old DI wrapper, missing).
    Also applies _dedup_spacejoined to all string field values.
    """
    if not isinstance(pw_fields, dict):
        return pw_fields

    line_items = pw_fields.get("LineItems")
    raw_items: List[Any] = []

    if isinstance(line_items, list):
        # Case A: already a list — may still need field-level unwrapping
        raw_items = line_items
    elif isinstance(line_items, dict) and "valueArray" in line_items:
        # Case B: top-level DI array wrapper
        for item in (line_items.get("valueArray") or []):
            if not isinstance(item, dict):
                continue
            vo = item.get("valueObject", {})
            raw_items.append(vo if isinstance(vo, dict) else item)
    else:
        # Case C: missing / None / unexpected
        pw_fields["LineItems"] = []
        return pw_fields

    clean_items = []
    for item in raw_items:
        if not isinstance(item, dict):
            continue
        row = {}
        for field_name, field_val in item.items():
            unwrapped = _unwrap_line_item_field(field_val)
            # Deduplicate space-joined repeated values from DI multi-span extraction
            if isinstance(unwrapped, str):
                unwrapped = _dedup_spacejoined(unwrapped)
            row[field_name] = unwrapped
        clean_items.append(row)

    pw_fields["LineItems"] = clean_items
    return pw_fields


# -------------------------
# PDF HELPERS
# -------------------------
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


# -------------------------
# INVOICE DATE PARSING / NORMALIZATION
# -------------------------
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
    if len(parts) == 3 and len(parts[0]) == 4 and parts[0].isdigit():
        y, m, d = parts[0], parts[1].zfill(2), parts[2].zfill(2)
        if m.isdigit() and d.isdigit():
            return f"{y}-{m}-{d}"
    if len(parts) == 3 and len(parts[2]) == 4 and parts[2].isdigit():
        a, b, y = parts[0], parts[1], parts[2]
        if a.isdigit() and b.isdigit():
            da, db = int(a), int(b)
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
        first_this_month = today.replace(day=1)
        last_prev_month = first_this_month - timedelta(days=1)
        first_prev_month = last_prev_month.replace(day=1)
        return (first_prev_month.strftime("%Y-%m-%d"), last_prev_month.strftime("%Y-%m-%d"))
    return (None, None)


# -------------------------
# SUMMARY COLUMNS
# -------------------------
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
    with ", ". Used so all parts are visible in summary columns, not just row 0.
    e.g. ["6492804", "6492806", "6585584"] => "6492804, 6492806, 6585584"
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


# -------------------------
# HTTP: UPLOAD (PDF + ZIP)
# -------------------------
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
    batch_id = str(uuid.uuid4())
    jobs = []

    def enqueue_one_pdf(pdf_bytes: bytes, filename: str) -> dict:
        job_id = str(uuid.uuid4())
        blob_path = f"{job_id}/original/{filename}"
        uploads.upload_blob(blob_path, pdf_bytes, overwrite=True)
        qc.send_message(json.dumps({"jobId": job_id, "pdfBlobPath": blob_path, "fileName": filename}))
        return {"jobId": job_id, "fileName": filename, "blobPath": blob_path, "bytes": len(pdf_bytes)}

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
                    status_code=400, mimetype="application/json",
                )

        resp = {"batchId": batch_id, "jobs": jobs}
        if len(jobs) == 1:
            resp["jobId"] = jobs[0]["jobId"]
        return func.HttpResponse(json.dumps(resp), status_code=200, mimetype="application/json")

    except zipfile.BadZipFile:
        return func.HttpResponse(json.dumps({"error": "Invalid ZIP file."}), status_code=400, mimetype="application/json")
    except Exception as e:
        return func.HttpResponse(json.dumps({"error": str(e)}), status_code=500, mimetype="application/json")


# -------------------------
# QUEUE WORKER
# -------------------------
@app.function_name(name="job_worker")
@app.queue_trigger(arg_name="msg", queue_name="jobs", connection="AzureWebJobsStorage")
def job_worker(msg: func.QueueMessage) -> None:
    payload = json.loads(msg.get_body().decode("utf-8"))
    job_id = payload.get("jobId")
    pdf_blob_path = payload.get("pdfBlobPath")
    file_name = payload.get("fileName")

    container = cosmos_container()
    uploads = blob_service().get_container_client(UPLOADS_CONTAINER)
    created_at = utc_now_iso()

    try:
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
        pages = split_pdf_to_single_pages(pdf_bytes)
        total_pages = len(pages)
        client = di_client()

        # 1) Classify each page
        page_results = []
        failures = []
        for item in pages:
            pno = item["pageNumber"]
            one_pdf = item["pdfBytes"]
            try:
                poller = client.begin_classify_document(
                    CLASSIFIER_ID, body=one_pdf, content_type="application/pdf",
                )
                result = poller.result()
                docs = getattr(result, "documents", None) or []
                if docs:
                    d0 = docs[0]
                    doc_type = getattr(d0, "doc_type", None) or getattr(d0, "docType", None)
                    conf = getattr(d0, "confidence", None)
                else:
                    doc_type, conf = None, None
                page_results.append({"pageNumber": pno, "docType": doc_type, "confidence": conf, "pdf": one_pdf})
            except Exception as e:
                failures.append({"pageNumber": pno, "error": str(e)})

        # 2) Pick best page per target doc type
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

        # 3) Extract fields
        merged = {}
        picked_pages = {}
        confidence = {}
        extracted_docs = []

        for doc_type, info in best_pages.items():
            model_id = EXTRACTION_MODELS.get(doc_type)
            if not model_id:
                continue

            def analyze(pdf_bytes_to_use: bytes):
                poller = client.begin_analyze_document(
                    model_id, body=pdf_bytes_to_use, content_type="application/pdf",
                )
                return poller.result()

            analyze_result = analyze(info["pdf"])
            simplified = simplify_analyze_result(analyze_result)
            used_rotation = 0

            # Rotation fallback for PARTS WORKSHEET
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
            fields = simplified.get("fields", {})

            # FIX: Normalize parts_worksheet LineItems into a clean plain list.
            # Handles DI wrapper objects, content-only fields, and deduplicates
            # space-joined repeated values from multi-span OCR extraction.
            if key == "parts_worksheet":
                fields = _fix_parts_worksheet_line_items(fields)

            merged[key] = fields
            picked_pages[doc_type] = info.get("pageNumber")
            confidence[doc_type] = info.get("confidence")
            extracted_docs.append({
                "docType": doc_type,
                "modelId": model_id,
                "page": info.get("pageNumber"),
                "rotationAppliedDegrees": used_rotation,
                "fieldCount": len(fields),
            })

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
            "confidence": confidence,
            "extractedDocs": extracted_docs,
            **merged,
        }
        final_doc["cevaInvoiceDate"] = _extract_ceva_invoice_date(final_doc)
        container.upsert_item(final_doc)
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


# -------------------------
# HTTP: JOB UI (UI-READY)
# -------------------------
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
    if status in ("processing", "failed"):
        out = {"id": job_id, "status": status}
        if status == "processing":
            out["createdAt"] = doc.get("createdAt")
        if status == "failed":
            out["error"] = doc.get("error")
            out["failedAt"] = doc.get("failedAt")
        return func.HttpResponse(json.dumps(out, default=str), status_code=200, mimetype="application/json")

    def strip_internal(d: dict) -> dict:
        if not isinstance(d, dict):
            return d
        return {k: v for k, v in d.items() if not k.startswith("_")}

    # Apply fix when serving old Cosmos docs so the detail view also renders
    # correctly without needing reprocessing.
    pw_raw = strip_internal(doc.get("parts_worksheet") or {})
    pw_fixed = _fix_parts_worksheet_line_items(pw_raw)

    out = {
        "id": doc.get("id"),
        "fileName": doc.get("fileName"),
        "status": doc.get("status"),
        "createdAt": doc.get("createdAt"),
        "completedAt": doc.get("completedAt"),
        "cevaInvoiceDate": doc.get("cevaInvoiceDate"),
        "ceva": strip_internal(doc.get("ceva") or {}),
        "entry_summary": strip_internal(doc.get("entry_summary") or {}),
        "parts_worksheet": pw_fixed,
    }
    return func.HttpResponse(json.dumps(out, default=str), status_code=200, mimetype="application/json")


# -------------------------
# COSMOS QUERIES (LIST + EXCEL)
# -------------------------
def _build_filters_from_req(req: func.HttpRequest):
    q = req.params.get("q")
    range_key = req.params.get("range")
    invoice_from = _parse_iso_date_only(req.params.get("invoiceFrom"))
    invoice_to = _parse_iso_date_only(req.params.get("invoiceTo"))

    if range_key and (not invoice_from and not invoice_to) and range_key.strip().lower() != "all":
        rf, rt = _compute_range(range_key)
        invoice_from, invoice_to = rf, rt

    where = ['c.type = "bundle_result"', 'c.status = "completed"']
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


# -------------------------
# HTTP: RESULTS LIST (PAGINATED)
# Returns full LineItems array from Cosmos, then post-processes server-side:
#   - fixes any wrapped LineItems (old docs)
#   - builds comma-joined summary columns across ALL line items (not just [0])
# -------------------------
@app.route(route="results", methods=["GET"])
def list_results(req: func.HttpRequest) -> func.HttpResponse:
    container = cosmos_container()

    page_size = _parse_int(req.params.get("pageSize"), DEFAULT_PAGE_SIZE)
    page_size = max(1, min(MAX_PAGE_SIZE, page_size))
    token = req.params.get("token")

    where_clause, params = _build_filters_from_req(req)

    # Pull full LineItems array so we can join all values, not just [0]
    query = f"""
    SELECT
      c.id,
      c.fileName,
      c.cevaInvoiceDate,
      c.ceva["INVOICE NUMBER"] AS invoiceNumber,
      c.ceva["ENTRY NUMBER"] AS entryNumber,
      c.ceva["DEPARTURE DATE"] AS departureDate,
      c.ceva["ARRIVAL DATE"] AS arrivalDate,
      c.ceva["DESCRIPTION OF GOODS"] AS descriptionOfGoods,
      c.entry_summary.Total_Entered_Value AS totalEnteredValue,
      c.entry_summary.Total_Other_Fees AS totalOtherFees,
      c.entry_summary.Duty AS duty,
      c.parts_worksheet.Supplier AS supplier,
      c.parts_worksheet.LineItems AS lineItems
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
        page_iter = items_iterable.by_page(continuation_token=token)
        page = next(page_iter)
        raw_items = list(page)
        next_token = getattr(page, "continuation_token", None)

        # Post-process: fix wrapped LineItems (old docs), build comma-joined columns
        items = []
        for item in raw_items:
            li_raw = item.pop("lineItems", None) or []
            pw_shell = _fix_parts_worksheet_line_items({"LineItems": li_raw})
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
        return func.HttpResponse(json.dumps({"error": str(e)}), mimetype="application/json", status_code=500)


# -------------------------
# EXCEL EXPORT
# Summary sheet: all parts joined with ", " (consistent with UI table)
# LineItems sheet: one row per line item (full granularity)
# -------------------------
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
            query=query, parameters=params,
            enable_cross_partition_query=True, max_item_count=200,
        )
        all_docs = []
        for page in items_iterable.by_page():
            all_docs.extend(list(page))

        wb = Workbook()
        ws = wb.active
        ws.title = "Summary"
        ws.append([label for _, label in SUMMARY_COLS])

        ws_li = wb.create_sheet("LineItems")
        ws_li.append(["File Name", "CEVA INVOICE DATE", "Invoice_Seq",
                       "Invoice_Number", "Part_No", "HTS_code", "Description_of_Goods"])

        for d in all_docs:
            ceva = d.get("ceva") if isinstance(d.get("ceva"), dict) else {}
            es   = d.get("entry_summary") if isinstance(d.get("entry_summary"), dict) else {}
            pw   = d.get("parts_worksheet") if isinstance(d.get("parts_worksheet"), dict) else {}

            # Normalize LineItems at export time — fixes old docs without reprocessing
            pw = _fix_parts_worksheet_line_items(dict(pw))
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

        _autosize_columns(ws, len(SUMMARY_COLS))
        _autosize_columns(ws_li, 7)

        out = io.BytesIO()
        wb.save(out)
        out.seek(0)

        range_key    = (req.params.get("range") or "all").strip()
        invoice_from = _parse_iso_date_only(req.params.get("invoiceFrom"))
        invoice_to   = _parse_iso_date_only(req.params.get("invoiceTo"))
        suffix = range_key.lower() if range_key else "all"
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
        return func.HttpResponse(json.dumps({"error": str(e)}), mimetype="application/json", status_code=500)
