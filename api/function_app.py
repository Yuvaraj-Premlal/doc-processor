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

TARGET_DOC_TYPES = ["CEVA", "ENTRY SUMMARY", "PARTS WORKSHEET"]

EXTRACTION_MODELS = {
    "CEVA": "ceva_invoice_model",
    "ENTRY SUMMARY": "entry-summary-v1",
    "PARTS WORKSHEET": "partsworksheet_model",
}

ROTATION_FALLBACK_DEGREES = -90

DEFAULT_PAGE_SIZE = 20
MAX_PAGE_SIZE = 200

# Exact field name inside CEVA model
CEVA_INVOICE_DATE_FIELD = "INVOICE DATE"


# =========================
# Time helpers
# =========================
def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_now_iso() -> str:
    return utc_now().isoformat()


# =========================
# Azure Clients
# =========================
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


# =========================
# Storage cleanup
# =========================
def delete_prefix(container_client, prefix: str):
    for blob in container_client.list_blobs(name_starts_with=prefix):
        container_client.delete_blob(blob.name)


# =========================
# General utilities
# =========================
def normalize_doctype(s: Optional[str]) -> str:
    return (s or "").strip().upper()


def safe_slug(s: str) -> str:
    return (s or "").strip().upper().replace(" ", "_")


def _is_pdf_filename(name: str) -> bool:
    return (name or "").lower().endswith(".pdf")


def _is_zip_filename(name: str) -> bool:
    return (name or "").lower().endswith(".zip")


# =========================
# PDF helpers
# =========================
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


# =========================
# Document Intelligence -> JSON
# =========================
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
    """
    Try to convert DocumentField to a useful primitive.
    IMPORTANT: Some list/array fields may still come through as DI wrapper dicts.
    We'll normalize LineItems later specifically.
    """
    if field is None:
        return None

    value = getattr(field, "value", None)
    if value is not None:
        return _jsonable(value)

    content = getattr(field, "content", None)
    if content is not None:
        return content

    return _jsonable(field)


def simplify_analyze_result(result) -> Dict[str, Any]:
    docs = getattr(result, "documents", None) or []
    if not docs:
        return {"fields": {}, "hasData": False}

    fields = getattr(docs[0], "fields", None) or {}
    simple = {}
    for k, v in fields.items():
        simple[str(k)] = _field_value_simple(v)

    has_data = any(v not in (None, "", [], {}) for v in simple.values())
    return {"fields": simple, "hasData": has_data}


# =========================
# LineItems normalization (CRITICAL FIX)
# =========================
def _unwrap_typed_value(v: Any) -> Any:
    """
    Handles DI wrapper objects like:
      {"type":"string","valueString":"SEQ 1","content":"SEQ 1",...}
    """
    if isinstance(v, dict):
        # common DI keys:
        for key in ("valueString", "valueNumber", "valueInteger", "valueDate", "valueTime", "valuePhoneNumber"):
            if key in v:
                return v.get(key)
        # sometimes valueObject/valueArray wrappers
        if "valueObject" in v and isinstance(v["valueObject"], dict):
            return {k: _unwrap_typed_value(val) for k, val in v["valueObject"].items()}
        if "valueArray" in v and isinstance(v["valueArray"], list):
            return [_unwrap_typed_value(x) for x in v["valueArray"]]
        if "content" in v and v.get("content") not in (None, ""):
            return v.get("content")
        # fallback: try stripping geometry-like keys
        drop = {"boundingRegions", "polygon", "spans"}
        return {k: _unwrap_typed_value(val) for k, val in v.items() if k not in drop}

    if isinstance(v, list):
        return [_unwrap_typed_value(x) for x in v]

    return v


def normalize_parts_worksheet_fields(fields: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convert LineItems into a REAL list[dict] so UI and Excel work.
    Your current Cosmos doc shows LineItems as:
      {"type":"array","valueArray":[{"type":"object","valueObject":{...}}]}
    We convert it to:
      [{"Invoice_Seq":"SEQ 1", "Invoice_Number":"EXP-0005", ...}, ...]
    """
    if not isinstance(fields, dict):
        return fields

    if "LineItems" not in fields:
        return fields

    li = fields.get("LineItems")

    # If already a list -> good
    if isinstance(li, list):
        fields["LineItems"] = [_unwrap_typed_value(x) for x in li]
        return fields

    # If DI wrapper dict -> unwrap
    if isinstance(li, dict) and "valueArray" in li and isinstance(li["valueArray"], list):
        out_items = []
        for item in li["valueArray"]:
            # item is usually {"type":"object","valueObject":{...}}
            if isinstance(item, dict) and "valueObject" in item and isinstance(item["valueObject"], dict):
                out_items.append(_unwrap_typed_value(item["valueObject"]))
            else:
                out_items.append(_unwrap_typed_value(item))
        fields["LineItems"] = out_items
        return fields

    # fallback
    fields["LineItems"] = _unwrap_typed_value(li)
    return fields


# =========================
# Flattening
# =========================
def flatten_for_row(prefix: str, obj: Any, out: dict):
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
    out[prefix] = str(obj)


# =========================
# Invoice Date helpers
# =========================
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

    # Try "27-Apr-2025"
    for fmt in ("%d-%b-%Y", "%d-%B-%Y", "%d %b %Y", "%d %B %Y"):
        try:
            dt = datetime.strptime(s.replace("/", "-").replace("  ", " ").strip(), fmt)
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


def _parse_iso_date_only(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    s = s.strip()
    if len(s) >= 10 and s[4] == "-" and s[7] == "-":
        return s[:10]
    return None


def _compute_created_range(created_range: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    """
    createdRange filter applies to createdAt (stored as ISO string).
    Return (from_iso, to_iso) inclusive-ish bounds.
    """
    if not created_range or created_range.strip().lower() == "all":
        return (None, None)

    rk = created_range.strip().lower()
    now = utc_now()
    if rk == "last_7_days":
        start = now - timedelta(days=7)
        return (start.isoformat(), now.isoformat())
    if rk == "last_30_days":
        start = now - timedelta(days=30)
        return (start.isoformat(), now.isoformat())
    if rk == "last_month":
        # previous calendar month
        first_this = now.date().replace(day=1)
        last_prev = first_this - timedelta(days=1)
        first_prev = last_prev.replace(day=1)
        start = datetime(first_prev.year, first_prev.month, first_prev.day, tzinfo=timezone.utc)
        end = datetime(last_prev.year, last_prev.month, last_prev.day, 23, 59, 59, tzinfo=timezone.utc)
        return (start.isoformat(), end.isoformat())

    return (None, None)


# =========================
# Upload Endpoint (PDF(s) + ZIP)
# =========================
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
        if not job_id or not pdf_blob_path:
            raise ValueError("Queue payload missing jobId or pdfBlobPath")

        # mark processing
        container.upsert_item({
            "id": job_id,
            "type": "bundle_result",
            "fileName": file_name,
            "createdAt": created_at,
            "status": "processing",
        })

        pdf_bytes = uploads.get_blob_client(pdf_blob_path).download_blob().readall()
        reader = PdfReader(io.BytesIO(pdf_bytes))
        total_pages = len(reader.pages)

        client = di_client()
        page_results = []
        failures = []

        # -------- CLASSIFICATION --------
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

        # -------- PICK BEST PAGE --------
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

        # -------- EXTRACTION --------
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

            # Rotation fallback for Parts Worksheet if empty
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

            fields = simplified.get("fields", {})

            # CRITICAL: normalize parts worksheet line items into real list
            if normalize_doctype(doc_type) == "PARTS WORKSHEET":
                fields = normalize_parts_worksheet_fields(fields)

            key = safe_slug(doc_type).lower()
            merged[key] = fields
            flatten_for_row(key, fields, flattened)

            picked_pages[doc_type] = info.get("pageNumber")
            confidence[doc_type] = info.get("confidence")

            extracted_docs.append({
                "docType": doc_type,
                "modelId": model_id,
                "page": info.get("pageNumber"),
                "rotationAppliedDegrees": used_rotation,
                "fieldCount": len(fields) if isinstance(fields, dict) else 0,
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

        # top-level query field
        final_doc["cevaInvoiceDate"] = _extract_ceva_invoice_date(final_doc)

        container.upsert_item(final_doc)

        # cleanup uploads
        delete_prefix(uploads, f"{job_id}/")

    except Exception as e:
        # record failure
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
# UI Endpoint (clean JSON for frontend)
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
    if status in ("processing", "failed"):
        return func.HttpResponse(json.dumps(doc, default=str), status_code=200, mimetype="application/json")

    # ensure parts worksheet lineitems are normalized even for older stored docs
    pw = doc.get("parts_worksheet") if isinstance(doc.get("parts_worksheet"), dict) else {}
    pw = normalize_parts_worksheet_fields(pw)
    doc["parts_worksheet"] = pw

    return func.HttpResponse(json.dumps(doc, default=str), status_code=200, mimetype="application/json")


# =========================
# Results Listing (Pagination + Filters)
# GET /api/results?pageSize=20&token=...&createdRange=last_7_days&invoiceFrom=YYYY-MM-DD&invoiceTo=YYYY-MM-DD
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

    created_range = _get_query_param(req, "createdRange", "all")
    created_from_iso, created_to_iso = _compute_created_range(created_range)

    invoice_from = _parse_iso_date_only(_get_query_param(req, "invoiceFrom", None))
    invoice_to = _parse_iso_date_only(_get_query_param(req, "invoiceTo", None))

    where = ['c.type = "bundle_result"']
    params = []

    # createdAt filter (quick filters)
    if created_from_iso:
        where.append("c.createdAt >= @createdFrom")
        params.append({"name": "@createdFrom", "value": created_from_iso})
    if created_to_iso:
        where.append("c.createdAt <= @createdTo")
        params.append({"name": "@createdTo", "value": created_to_iso})

    # invoice date filter
    if invoice_from:
        where.append("IS_DEFINED(c.cevaInvoiceDate) AND c.cevaInvoiceDate >= @invFrom")
        params.append({"name": "@invFrom", "value": invoice_from})
    if invoice_to:
        where.append("IS_DEFINED(c.cevaInvoiceDate) AND c.cevaInvoiceDate <= @invTo")
        params.append({"name": "@invTo", "value": invoice_to})

    where_clause = " AND ".join(where)

    # IMPORTANT: include flattened so UI can display your extracted columns
    query = f"""
    SELECT c.id, c.fileName, c.cevaInvoiceDate, c.flattened
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

        # ✅ FIX: by_page() does NOT take max_item_count
        page_iter = items_iterable.by_page(continuation_token=token)
        page = next(page_iter, [])
        items = list(page)

        next_token = getattr(page, "continuation_token", None)

        return func.HttpResponse(
            json.dumps({"items": items, "continuationToken": next_token}, default=str),
            mimetype="application/json",
            status_code=200,
        )

    except Exception as e:
        return func.HttpResponse(json.dumps({"error": str(e)}), mimetype="application/json", status_code=500)


# =========================
# Excel Export (ALL matching docs)
# GET /api/results/excel?createdRange=...&invoiceFrom=...&invoiceTo=...
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


def _excel_safe(v: Any) -> Any:
    """
    openpyxl cannot store dict/list objects in a cell.
    Convert to string when needed.
    """
    if v is None:
        return ""
    if isinstance(v, (str, int, float, bool)):
        return v
    return json.dumps(v, ensure_ascii=False)


@app.route(route="results/excel", methods=["GET"])
def export_results_excel(req: func.HttpRequest) -> func.HttpResponse:
    container = cosmos_container()

    created_range = _get_query_param(req, "createdRange", "all")
    created_from_iso, created_to_iso = _compute_created_range(created_range)

    invoice_from = _parse_iso_date_only(_get_query_param(req, "invoiceFrom", None))
    invoice_to = _parse_iso_date_only(_get_query_param(req, "invoiceTo", None))

    where = ['c.type = "bundle_result"']
    params = []

    if created_from_iso:
        where.append("c.createdAt >= @createdFrom")
        params.append({"name": "@createdFrom", "value": created_from_iso})
    if created_to_iso:
        where.append("c.createdAt <= @createdTo")
        params.append({"name": "@createdTo", "value": created_to_iso})

    if invoice_from:
        where.append("IS_DEFINED(c.cevaInvoiceDate) AND c.cevaInvoiceDate >= @invFrom")
        params.append({"name": "@invFrom", "value": invoice_from})
    if invoice_to:
        where.append("IS_DEFINED(c.cevaInvoiceDate) AND c.cevaInvoiceDate <= @invTo")
        params.append({"name": "@invTo", "value": invoice_to})

    where_clause = " AND ".join(where)

    # pull everything needed for Excel
    query = f"""
    SELECT c.id, c.fileName, c.cevaInvoiceDate, c.flattened, c.parts_worksheet
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

        wb = Workbook()
        ws = wb.active
        ws.title = "Jobs"

        # Your requested columns (from flattened)
        # NOTE: these keys exist in doc["flattened"] (exactly as you showed)
        wanted_flat_cols = [
            "ceva.INVOICE NUMBER",
            "ceva.ENTRY NUMBER",
            "ceva.INVOICE DATE",
            "ceva.DEPARTURE DATE",
            "ceva.ARRIVAL DATE",
            "ceva.DESCRIPTION OF GOODS",
            "entry_summary.Total_Entered_Value",
            "entry_summary.Total_Other_Fees",
            "entry_summary.Duty",
            "parts_worksheet.Supplier",
        ]

        headers = ["File Name", "CEVA INVOICE DATE"] + wanted_flat_cols
        ws.append(headers)

        for d in all_docs:
            flat = d.get("flattened") if isinstance(d.get("flattened"), dict) else {}
            row = [
                d.get("fileName", ""),
                d.get("cevaInvoiceDate", ""),
            ]
            for k in wanted_flat_cols:
                row.append(_excel_safe(flat.get(k)))
            ws.append(row)

        _autosize_columns(ws, len(headers))

        # LineItems sheet
        ws2 = wb.create_sheet("LineItems")
        ws2.append(["File Name", "CEVA INVOICE DATE", "Invoice_Seq", "Invoice_Number", "Part_No", "HTS_code", "Description_of_Goods"])

        for d in all_docs:
            pw = d.get("parts_worksheet") if isinstance(d.get("parts_worksheet"), dict) else {}
            pw = normalize_parts_worksheet_fields(pw)
            items = pw.get("LineItems", [])
            if not isinstance(items, list):
                items = []

            for it in items:
                it = it if isinstance(it, dict) else {}
                ws2.append([
                    d.get("fileName", ""),
                    d.get("cevaInvoiceDate", ""),
                    _excel_safe(it.get("Invoice_Seq")),
                    _excel_safe(it.get("Invoice_Number")),
                    _excel_safe(it.get("Part_No")),
                    _excel_safe(it.get("HTS_code")),
                    _excel_safe(it.get("Description_of_Goods")),
                ])

        _autosize_columns(ws2, 7)

        out = io.BytesIO()
        wb.save(out)
        out.seek(0)

        filename = "bundle_export.xlsx"
        headers_resp = {"Content-Disposition": f'attachment; filename="{filename}"'}

        return func.HttpResponse(
            body=out.getvalue(),
            status_code=200,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers=headers_resp,
        )

    except Exception as e:
        return func.HttpResponse(json.dumps({"error": str(e)}), mimetype="application/json", status_code=500)
