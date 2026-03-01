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

# -------------------------
# CONFIG
# -------------------------
TARGET_DOC_TYPES = ["CEVA", "ENTRY SUMMARY", "PARTS WORKSHEET"]

EXTRACTION_MODELS = {
    "CEVA": "ceva_invoice_model",
    "ENTRY SUMMARY": "entry-summary-v1",
    "PARTS WORKSHEET": "partsworksheet_model",
}

CLASSIFIER_ID = "cevadocclassifier"

ROTATION_FALLBACK_DEGREES = -90

DEFAULT_PAGE_SIZE = 20
MAX_PAGE_SIZE = 200

CEVA_INVOICE_DATE_FIELD = "INVOICE DATE"

# Columns you want in the list table + Excel (main sheet)
SUMMARY_COLUMNS = [
    ("fileName", "File Name"),

    ("cevaInvoiceDate", "CEVA INVOICE DATE"),
    ("ceva.INVOICE NUMBER", "INVOICE NUMBER"),
    ("ceva.ENTRY NUMBER", "ENTRY NUMBER"),
    ("ceva.DEPARTURE DATE", "DEPARTURE DATE"),
    ("ceva.ARRIVAL DATE", "ARRIVAL DATE"),
    ("ceva.DESCRIPTION OF GOODS", "DESCRIPTION OF GOODS"),

    ("entry_summary.Total_Entered_Value", "TOTAL ENTERED VALUE"),
    ("entry_summary.Total_Other_Fees", "TOTAL OTHER FEES"),
    ("entry_summary.Duty", "DUTY"),

    ("parts_worksheet.Supplier", "SUPPLIER"),

    # Parts summary (joined from line items)
    ("partsSummary.Invoice_Seq", "PARTS Invoice_Seq"),
    ("partsSummary.Invoice_Number", "PARTS Invoice_Number"),
    ("partsSummary.Part_No", "PARTS Part_No"),
    ("partsSummary.HTS_code", "PARTS HTS_code"),
    ("partsSummary.Description_of_Goods", "PARTS Description_of_Goods"),
]


# -------------------------
# CORE HELPERS
# -------------------------
def utc_now() -> datetime:
    return datetime.now(timezone.utc)

def utc_now_iso() -> str:
    return utc_now().isoformat()

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
    db = client.get_database_client(os.environ.get("COSMOS_DATABASE", "pdfbundle"))
    return db.get_container_client(os.environ.get("COSMOS_CONTAINER", "results"))

def delete_prefix(container_client, prefix: str):
    for blob in container_client.list_blobs(name_starts_with=prefix):
        container_client.delete_blob(blob.name)


# -------------------------
# UTILS
# -------------------------
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


# -------------------------
# IMPORTANT: DI NORMALIZER
# Turns DI "typed" objects into plain JSON values
# -------------------------
def di_plain(value: Any) -> Any:
    """
    Converts Document Intelligence typed field structures into plain JSON.
    Handles patterns like:
      {"type":"string","valueString":"..."}
      {"type":"array","valueArray":[...]}
      {"type":"object","valueObject":{...}}
    Also drops geometry keys.
    """
    if value is None:
        return None

    if isinstance(value, (str, int, float, bool)):
        return value

    if isinstance(value, list):
        return [di_plain(v) for v in value]

    if isinstance(value, dict):
        # unwrap common DI typed fields
        if "valueString" in value:
            return value.get("valueString")
        if "valueNumber" in value:
            return value.get("valueNumber")
        if "valueInteger" in value:
            return value.get("valueInteger")
        if "valueBoolean" in value:
            return value.get("valueBoolean")
        if "valueDate" in value:
            return value.get("valueDate")
        if "valueTime" in value:
            return value.get("valueTime")

        if "valueArray" in value and isinstance(value["valueArray"], list):
            out = []
            for it in value["valueArray"]:
                # many items are {"type":"object","valueObject":{...}}
                if isinstance(it, dict) and "valueObject" in it:
                    out.append(di_plain(it["valueObject"]))
                else:
                    out.append(di_plain(it))
            return out

        if "valueObject" in value and isinstance(value["valueObject"], dict):
            return {k: di_plain(v) for k, v in value["valueObject"].items()}

        # drop noisy geometry keys
        drop = {"boundingRegions", "polygon", "spans", "content"}
        cleaned = {}
        for k, v in value.items():
            if k in drop:
                continue
            cleaned[k] = di_plain(v)
        return cleaned

    # fallback for sdk objects
    return di_plain(_jsonable(value))


def simplify_analyze_result(result) -> Dict[str, Any]:
    docs = getattr(result, "documents", None) or []
    if not docs:
        return {"fields": {}, "hasData": False}

    fields = getattr(docs[0], "fields", None) or {}
    simple = {}
    for k, v in fields.items():
        # v is typically DocumentField; convert to dict, then to plain
        vj = _jsonable(v)
        simple[str(k)] = di_plain(vj)

    has_data = any(x not in (None, "", [], {}) for x in simple.values())
    return {"fields": simple, "hasData": has_data}


def flatten_for_row(prefix: str, obj: Any, out: Dict[str, Any]):
    if obj is None:
        return
    if isinstance(obj, (str, int, float, bool)):
        out[prefix] = obj
    elif isinstance(obj, dict):
        for k, v in obj.items():
            flatten_for_row(f"{prefix}.{k}", v, out)
    elif isinstance(obj, list):
        # store lists as joined strings for flat summary (Excel/table)
        # (full list still exists in the main document)
        joined = " | ".join([str(di_plain(x)) for x in obj if x is not None])
        out[prefix] = joined
    else:
        out[prefix] = str(obj)


# -------------------------
# DATE PARSING (Invoice)
# -------------------------
def _try_parse_date_to_yyyy_mm_dd(value: Any) -> Optional[str]:
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None

    # ISO
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
        return _try_parse_date_to_yyyy_mm_dd(ceva.get(CEVA_INVOICE_DATE_FIELD))

    for k, v in ceva.items():
        lk = str(k).lower()
        if "invoice" in lk and "date" in lk:
            parsed = _try_parse_date_to_yyyy_mm_dd(v)
            if parsed:
                return parsed
    return None


# -------------------------
# FILTER HELPERS
# -------------------------
def _get_query_param(req: func.HttpRequest, name: str, default=None):
    v = req.params.get(name)
    return v if v is not None else default

def _parse_int(s, default: int):
    try:
        return int(s)
    except Exception:
        return default

def _parse_yyyy_mm_dd(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    s = s.strip()
    if len(s) >= 10 and s[4] == "-" and s[7] == "-":
        return s[:10]
    return None

def _compute_range(range_key: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    """
    Quick filters for invoice date.
    """
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


# -------------------------
# PARTS SUMMARY (for table + excel)
# -------------------------
def make_parts_summary(parts_worksheet: Dict[str, Any]) -> Dict[str, str]:
    """
    Creates summary columns from LineItems:
      - joins unique values with " | "
      - safe for table + Excel
    """
    out = {
        "Invoice_Seq": "",
        "Invoice_Number": "",
        "Part_No": "",
        "HTS_code": "",
        "Description_of_Goods": "",
    }

    if not isinstance(parts_worksheet, dict):
        return out

    li = parts_worksheet.get("LineItems")
    if not isinstance(li, list):
        return out

    def gather(key: str) -> str:
        vals = []
        for item in li:
            if isinstance(item, dict):
                v = item.get(key)
                if v is None:
                    continue
                s = str(v).strip()
                if s and s not in vals:
                    vals.append(s)
        return " | ".join(vals)

    out["Invoice_Seq"] = gather("Invoice_Seq")
    out["Invoice_Number"] = gather("Invoice_Number")
    out["Part_No"] = gather("Part_No")
    out["HTS_code"] = gather("HTS_code")
    out["Description_of_Goods"] = gather("Description_of_Goods")
    return out


# -------------------------
# UPLOAD (PDF + ZIP)
# -------------------------
@app.route(route="upload", methods=["POST"])
def upload(req: func.HttpRequest) -> func.HttpResponse:
    files = req.files.getlist("files")
    if not files:
        return func.HttpResponse(
            json.dumps({"error": "Upload files via multipart/form-data field 'files'"}),
            status_code=400,
            mimetype="application/json",
        )

    bsc = blob_service()
    uploads = bsc.get_container_client("uploads")
    try:
        uploads.create_container()
    except Exception:
        pass

    qc = queue_client()

    jobs = []
    batch_id = str(uuid.uuid4())

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
    uploads = blob_service().get_container_client("uploads")

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

        # classify each page
        for i, page in enumerate(reader.pages):
            writer = PdfWriter()
            writer.add_page(page)
            buf = io.BytesIO()
            writer.write(buf)
            one_page_pdf = buf.getvalue()

            try:
                poller = client.begin_classify_document(
                    CLASSIFIER_ID,
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
                        "pdf": one_page_pdf,
                    })
            except Exception as e:
                failures.append({"pageNumber": i + 1, "error": str(e)})

        # pick best page per target
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
        confidence = {}
        extracted_docs = []

        # extraction
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

            # rotation fallback only if no data
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
            merged[key] = simplified.get("fields", {})

            # flatten for summary keys
            flatten_for_row(key, simplified.get("fields", {}), flattened)

            picked_pages[doc_type] = info.get("pageNumber")
            confidence[doc_type] = info.get("confidence")

            extracted_docs.append({
                "docType": doc_type,
                "modelId": model_id,
                "page": info.get("pageNumber"),
                "rotationAppliedDegrees": used_rotation,
                "fieldCount": len(simplified.get("fields", {})),
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
            "flattened": flattened,
            "extractedDocs": extracted_docs,
            **merged,
        }

        # invoice date for filtering
        final_doc["cevaInvoiceDate"] = _extract_ceva_invoice_date(final_doc)

        # parts summary for table/excel
        pw = final_doc.get("parts_worksheet") if isinstance(final_doc.get("parts_worksheet"), dict) else {}
        final_doc["partsSummary"] = make_parts_summary(pw)

        container.upsert_item(final_doc)

        # cleanup uploads after success
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
# JOB UI ENDPOINT (polling)
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

    if status == "processing":
        return func.HttpResponse(json.dumps({"id": job_id, "status": "processing", "createdAt": doc.get("createdAt")}), status_code=200, mimetype="application/json")

    if status == "failed":
        return func.HttpResponse(json.dumps({"id": job_id, "status": "failed", "error": doc.get("error"), "failedAt": doc.get("failedAt")}, default=str), status_code=200, mimetype="application/json")

    # completed: return doc
    return func.HttpResponse(json.dumps(doc, default=str), status_code=200, mimetype="application/json")


# -------------------------
# RESULTS LIST (Pagination + Invoice filters)
# GET /api/results?pageSize=20&token=...&range=last_7_days&invoiceFrom=YYYY-MM-DD&invoiceTo=YYYY-MM-DD
# returns items with ONLY the keys needed by UI table (plus partsSummary)
# -------------------------
@app.route(route="results", methods=["GET"])
def list_results(req: func.HttpRequest) -> func.HttpResponse:
    container = cosmos_container()

    page_size = _parse_int(_get_query_param(req, "pageSize", str(DEFAULT_PAGE_SIZE)), DEFAULT_PAGE_SIZE)
    page_size = max(1, min(MAX_PAGE_SIZE, page_size))

    token = _get_query_param(req, "token", None)

    invoice_from = _parse_yyyy_mm_dd(_get_query_param(req, "invoiceFrom", None))
    invoice_to = _parse_yyyy_mm_dd(_get_query_param(req, "invoiceTo", None))
    range_key = _get_query_param(req, "range", "all")

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

    # pull only what UI needs (avoid huge docs)
    query = f"""
    SELECT
      c.id, c.fileName, c.cevaInvoiceDate,
      c.ceva, c.entry_summary, c.parts_worksheet,
      c.partsSummary
    FROM c
    WHERE {where_clause}
    ORDER BY c.cevaInvoiceDate DESC
    """

    try:
        items_iter = container.query_items(
            query=query,
            parameters=params,
            enable_cross_partition_query=True,
            max_item_count=page_size,
        )

        # IMPORTANT: your SDK error was because you passed max_item_count into by_page()
        page = next(items_iter.by_page(continuation_token=token))
        items = list(page)
        next_token = getattr(page, "continuation_token", None)

        return func.HttpResponse(
            json.dumps({"items": items, "continuationToken": next_token}, default=str),
            mimetype="application/json",
            status_code=200,
        )

    except Exception as e:
        return func.HttpResponse(json.dumps({"error": str(e)}), mimetype="application/json", status_code=500)


# -------------------------
# EXCEL EXPORT (ALL matching)
# GET /api/results/excel?range=...&invoiceFrom=...&invoiceTo=...
# - Main sheet = one row per document (your requested columns)
# - LineItems sheet = one row per part line item
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

def _get_nested(doc: dict, path: str):
    cur = doc
    for part in path.split("."):
        if not isinstance(cur, dict):
            return None
        cur = cur.get(part)
    return cur

@app.route(route="results/excel", methods=["GET"])
def export_results_excel(req: func.HttpRequest) -> func.HttpResponse:
    container = cosmos_container()

    invoice_from = _parse_yyyy_mm_dd(_get_query_param(req, "invoiceFrom", None))
    invoice_to = _parse_yyyy_mm_dd(_get_query_param(req, "invoiceTo", None))
    range_key = _get_query_param(req, "range", "all")

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
      c.id, c.fileName, c.cevaInvoiceDate,
      c.ceva, c.entry_summary, c.parts_worksheet,
      c.partsSummary
    FROM c
    WHERE {where_clause}
    ORDER BY c.cevaInvoiceDate DESC
    """

    try:
        items_iter = container.query_items(
            query=query,
            parameters=params,
            enable_cross_partition_query=True,
            max_item_count=200,
        )

        all_docs: List[dict] = []
        for pg in items_iter.by_page():
            all_docs.extend(list(pg))

        wb = Workbook()

        # ---- Main sheet (one row per document) ----
        ws = wb.active
        ws.title = "Bundles"

        headers = [h for _, h in SUMMARY_COLUMNS]
        ws.append(headers)

        for d in all_docs:
            row = []
            for key, _label in SUMMARY_COLUMNS:
                if "." in key:
                    row.append(_get_nested(d, key))
                else:
                    row.append(d.get(key))
            ws.append(row)

        _autosize_columns(ws, len(headers))

        # ---- LineItems sheet (one row per line item) ----
        ws2 = wb.create_sheet("LineItems")

        # Determine union of keys across line items (so you don't miss columns)
        union_keys = set()
        rows = []  # (jobId, fileName, invoiceDate, itemdict)

        for d in all_docs:
            pw = d.get("parts_worksheet") if isinstance(d.get("parts_worksheet"), dict) else {}
            li = pw.get("LineItems")
            if isinstance(li, list):
                for item in li:
                    if isinstance(item, dict):
                        union_keys.update(item.keys())
                        rows.append((d.get("id"), d.get("fileName"), d.get("cevaInvoiceDate"), item))

        item_cols = sorted(union_keys)
        ws2.append(["jobId", "fileName", "cevaInvoiceDate"] + item_cols)

        if rows:
            for jobId, fn, inv, item in rows:
                ws2.append([jobId, fn, inv] + [item.get(k) for k in item_cols])
            _autosize_columns(ws2, 3 + len(item_cols))
        else:
            ws2.append(["(no line items found)"])

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
