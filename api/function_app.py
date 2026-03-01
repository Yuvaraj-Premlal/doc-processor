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

from azure.cosmos import CosmosClient
from azure.cosmos.exceptions import CosmosResourceNotFoundError

from pypdf import PdfReader, PdfWriter


app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

TARGET_DOC_TYPES = ["CEVA", "ENTRY SUMMARY", "PARTS WORKSHEET"]

EXTRACTION_MODELS = {
    "CEVA": "ceva_invoice_model",
    "ENTRY SUMMARY": "entry-summary-v1",
    "PARTS WORKSHEET": "partsworksheet_model",
}

ROTATION_FALLBACK_DEGREES = -90

# Pagination defaults
DEFAULT_PAGE_SIZE = 50
MAX_PAGE_SIZE = 200


# =========================
# Core Helpers
# =========================

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


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
    client = CosmosClient(
        os.environ["COSMOS_ENDPOINT"],
        credential=os.environ["COSMOS_KEY"],
    )
    db = client.get_database_client(os.environ.get("COSMOS_DATABASE", "pdfbundle"))
    return db.get_container_client(os.environ.get("COSMOS_CONTAINER", "results"))


def delete_prefix(container_client, prefix: str):
    for blob in container_client.list_blobs(name_starts_with=prefix):
        container_client.delete_blob(blob.name)


# =========================
# Utility Helpers
# =========================

def normalize_doctype(s: str | None) -> str:
    return (s or "").strip().upper()


def safe_slug(s: str) -> str:
    return (s or "").strip().upper().replace(" ", "_")


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


def flatten_for_row(prefix, obj, out: dict):
    if obj is None:
        return
    if isinstance(obj, (str, int, float, bool)):
        out[prefix] = obj
    elif isinstance(obj, dict):
        for k, v in obj.items():
            flatten_for_row(f"{prefix}.{k}", v, out)
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            flatten_for_row(f"{prefix}[{i}]", v, out)
    else:
        out[prefix] = str(obj)


def _get_query_param(req: func.HttpRequest, name: str, default=None):
    v = req.params.get(name)
    return v if v is not None else default


def _parse_int(s, default: int):
    try:
        return int(s)
    except Exception:
        return default


def _parse_iso_date(s: str | None) -> str | None:
    """
    Accepts YYYY-MM-DD or full ISO. Returns ISO string (best-effort) or None.
    Cosmos compares strings lexicographically; ISO8601 UTC works well.
    """
    if not s:
        return None
    s = s.strip()
    # If YYYY-MM-DD, make it start-of-day UTC
    if len(s) == 10 and s[4] == "-" and s[7] == "-":
        return s + "T00:00:00+00:00"
    return s


# =========================
# Upload Endpoint
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

    job_id = str(uuid.uuid4())
    bsc = blob_service()
    uploads = bsc.get_container_client("uploads")
    try:
        uploads.create_container()
    except Exception:
        pass

    q = queue_client()

    uploaded = []
    for f in files:
        filename = getattr(f, "filename", "file.pdf")
        f.stream.seek(0)
        data = f.stream.read()

        blob_path = f"{job_id}/original/{filename}"
        uploads.upload_blob(blob_path, data, overwrite=True)

        msg = {"jobId": job_id, "pdfBlobPath": blob_path, "fileName": filename}
        q.send_message(json.dumps(msg))
        uploaded.append({"fileName": filename, "blobPath": blob_path, "bytes": len(data)})

    return func.HttpResponse(
        json.dumps({"jobId": job_id, "uploaded": uploaded}),
        mimetype="application/json",
        status_code=200,
    )


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
    uploads = blob_service().get_container_client("uploads")

    created_at = utc_now_iso()

    try:
        # Mark processing early (so UI can poll)
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

        # -------- CLASSIFICATION (in-memory per-page PDFs) --------
        for i, page in enumerate(reader.pages):
            writer = PdfWriter()
            writer.add_page(page)
            buf = io.BytesIO()
            writer.write(buf)
            single_pdf = buf.getvalue()

            try:
                poller = client.begin_classify_document(
                    "cevadocclassifier",
                    body=single_pdf,
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
                        "pdf": single_pdf,  # keep in-memory only
                    })
            except Exception as e:
                failures.append({"pageNumber": i + 1, "error": str(e)})

        # -------- BEST PAGE SELECTION --------
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

        # -------- EXTRACTION (in-memory) --------
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

            if normalize_doctype(doc_type) == "PARTS WORKSHEET" and not simplified.get("hasData", False):
                try:
                    rotated = rotate_pdf_bytes(info["pdf"], ROTATION_FALLBACK_DEGREES)
                    analyze2 = analyze(rotated)
                    simplified2 = simplify_analyze_result(analyze2)
                    if simplified2.get("hasData", False) and len(simplified2.get("fields", {})) >= len(simplified.get("fields", {})):
                        simplified = simplified2
                        used_rotation = ROTATION_FALLBACK_DEGREES
                except Exception:
                    # keep first attempt if rotation fails
                    pass

            key = safe_slug(doc_type).lower()
            merged[key] = simplified.get("fields", {})
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

        container.upsert_item(final_doc)

        # CLEANUP uploads (only after Cosmos write succeeds)
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
        # Let function fail so queue retry/poison settings apply as expected
        raise


# =========================
# UI Endpoint (Cosmos)
#   - Returns processing/failed/completed
# =========================

@app.route(route="job/{jobId}/ui", methods=["GET"])
def get_job_ui(req: func.HttpRequest) -> func.HttpResponse:
    job_id = req.route_params.get("jobId")
    if not job_id:
        return func.HttpResponse(
            json.dumps({"error": "Missing jobId"}),
            status_code=400,
            mimetype="application/json",
        )

    container = cosmos_container()

    try:
        doc = container.read_item(item=job_id, partition_key=job_id)
    except CosmosResourceNotFoundError:
        return func.HttpResponse(
            json.dumps({"id": job_id, "status": "not_found"}),
            status_code=404,
            mimetype="application/json",
        )

    status = doc.get("status")
    if status == "processing":
        return func.HttpResponse(
            json.dumps({"id": job_id, "status": "processing", "createdAt": doc.get("createdAt")}),
            status_code=200,
            mimetype="application/json",
        )

    if status == "failed":
        return func.HttpResponse(
            json.dumps({
                "id": job_id,
                "status": "failed",
                "error": doc.get("error"),
                "failedAt": doc.get("failedAt"),
            }, default=str),
            status_code=200,
            mimetype="application/json",
        )

    # completed (or unknown) -> return full doc
    return func.HttpResponse(
        json.dumps(doc, default=str),
        status_code=200,
        mimetype="application/json",
    )


# =========================
# Results Listing Endpoint (Cosmos) with Pagination
#
# GET /api/results?pageSize=50&token=...&status=completed&q=abc&from=YYYY-MM-DD&to=YYYY-MM-DD
#
# Response:
# {
#   "items": [...],
#   "continuationToken": "..." (or null)
# }
# =========================

@app.route(route="results", methods=["GET"])
def list_results(req: func.HttpRequest) -> func.HttpResponse:
    container = cosmos_container()

    # pageSize
    page_size = _parse_int(_get_query_param(req, "pageSize", str(DEFAULT_PAGE_SIZE)), DEFAULT_PAGE_SIZE)
    if page_size < 1:
        page_size = DEFAULT_PAGE_SIZE
    if page_size > MAX_PAGE_SIZE:
        page_size = MAX_PAGE_SIZE

    # continuation token from client (opaque string)
    token = _get_query_param(req, "token", None)

    # filters
    status = _get_query_param(req, "status", None)  # processing/completed/failed
    q = _get_query_param(req, "q", None)            # substring match on filename
    date_from = _parse_iso_date(_get_query_param(req, "from", None))
    date_to = _parse_iso_date(_get_query_param(req, "to", None))

    # Build query dynamically (parameterized)
    where = ['c.type = "bundle_result"']
    params = []

    if status:
        where.append("c.status = @status")
        params.append({"name": "@status", "value": status})

    if q:
        # Cosmos CONTAINS is case-sensitive; we can do LOWER() for case-insensitive
        where.append("CONTAINS(LOWER(c.fileName), LOWER(@q))")
        params.append({"name": "@q", "value": q})

    if date_from:
        where.append("c.createdAt >= @from")
        params.append({"name": "@from", "value": date_from})

    if date_to:
        where.append("c.createdAt <= @to")
        params.append({"name": "@to", "value": date_to})

    where_clause = " AND ".join(where)

    query = f"""
    SELECT c.id, c.fileName, c.createdAt, c.completedAt, c.status
    FROM c
    WHERE {where_clause}
    ORDER BY c.createdAt DESC
    """

    try:
        # query_items returns an ItemPaged; by_page handles continuation tokens
        items_iterable = container.query_items(
            query=query,
            parameters=params,
            enable_cross_partition_query=True,
            max_item_count=page_size,
        )

        page = next(items_iterable.by_page(continuation_token=token, max_item_count=page_size))
        items = list(page)
        next_token = getattr(page, "continuation_token", None)

        return func.HttpResponse(
            json.dumps({"items": items, "continuationToken": next_token}, default=str),
            mimetype="application/json",
            status_code=200,
        )

    except Exception as e:
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            mimetype="application/json",
            status_code=500,
        )
