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


# =========================
# Core Helpers
# =========================

def utc_now_iso():
    return datetime.now(timezone.utc).isoformat()


def blob_service():
    return BlobServiceClient.from_connection_string(os.environ["AzureWebJobsStorage"])


def queue_client():
    q = QueueClient.from_connection_string(
        os.environ["AzureWebJobsStorage"], "jobs"
    )
    try:
        q.create_queue()
    except:
        pass
    return q


def di_client():
    return DocumentIntelligenceClient(
        endpoint=os.environ["DI_ENDPOINT"],
        credential=AzureKeyCredential(os.environ["DI_KEY"]),
    )


def cosmos_container():
    client = CosmosClient(
        os.environ["COSMOS_ENDPOINT"],
        credential=os.environ["COSMOS_KEY"],
    )
    db = client.get_database_client(os.environ["COSMOS_DATABASE"])
    return db.get_container_client(os.environ["COSMOS_CONTAINER"])


def delete_prefix(container_client, prefix):
    for blob in container_client.list_blobs(name_starts_with=prefix):
        container_client.delete_blob(blob.name)


# =========================
# Utility Helpers
# =========================

def normalize_doctype(s):
    return (s or "").strip().upper()


def safe_slug(s):
    return (s or "").strip().upper().replace(" ", "_")


def rotate_pdf_bytes(pdf_bytes, degrees):
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
            return _jsonable(fn())
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
    docs = result.documents or []
    if not docs:
        return {"fields": {}, "hasData": False}

    fields = docs[0].fields or {}
    simple = {}
    for k, v in fields.items():
        simple[str(k)] = _field_value_simple(v)

    has_data = any(v not in (None, "", [], {}) for v in simple.values())
    return {"fields": simple, "hasData": has_data}


def flatten_for_row(prefix, obj, out):
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


# =========================
# Upload Endpoint
# =========================

@app.route(route="upload", methods=["POST"])
def upload(req: func.HttpRequest):

    files = req.files.getlist("files")
    if not files:
        return func.HttpResponse("Upload files via multipart/form-data field 'files'", status_code=400)

    job_id = str(uuid.uuid4())
    bsc = blob_service()
    uploads = bsc.get_container_client("uploads")
    try:
        uploads.create_container()
    except:
        pass

    q = queue_client()

    for f in files:
        data = f.stream.read()
        blob_path = f"{job_id}/original/{f.filename}"
        uploads.upload_blob(blob_path, data, overwrite=True)
        q.send_message(json.dumps({
            "jobId": job_id,
            "pdfBlobPath": blob_path,
            "fileName": f.filename
        }))

    return func.HttpResponse(json.dumps({"jobId": job_id}), mimetype="application/json")


# =========================
# Queue Worker
# =========================

@app.function_name(name="job_worker")
@app.queue_trigger(arg_name="msg", queue_name="jobs", connection="AzureWebJobsStorage")
def job_worker(msg: func.QueueMessage):

    payload = json.loads(msg.get_body().decode())
    job_id = payload["jobId"]
    pdf_blob_path = payload["pdfBlobPath"]
    file_name = payload.get("fileName")

    container = cosmos_container()
    uploads = blob_service().get_container_client("uploads")

    try:
        container.upsert_item({
            "id": job_id,
            "type": "bundle_result",
            "fileName": file_name,
            "createdAt": utc_now_iso(),
            "status": "processing"
        })

        pdf_bytes = uploads.get_blob_client(pdf_blob_path).download_blob().readall()
        reader = PdfReader(io.BytesIO(pdf_bytes))
        client = di_client()

        page_results = []
        failures = []

        # -------- CLASSIFICATION --------
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
                    content_type="application/pdf"
                )
                result = poller.result()
                docs = result.documents or []
                if docs:
                    page_results.append({
                        "pageNumber": i + 1,
                        "docType": docs[0].doc_type,
                        "confidence": docs[0].confidence,
                        "pdf": single_pdf
                    })
            except Exception as e:
                failures.append({"pageNumber": i + 1, "error": str(e)})

        # -------- BEST PAGE SELECTION --------
        best_pages = {}
        for target in TARGET_DOC_TYPES:
            matches = [
                r for r in page_results
                if normalize_doctype(r["docType"]) == normalize_doctype(target)
            ]
            matches.sort(key=lambda x: x["confidence"], reverse=True)
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

            analyze_result = client.begin_analyze_document(
                model_id,
                body=info["pdf"],
                content_type="application/pdf"
            ).result()

            simplified = simplify_analyze_result(analyze_result)
            used_rotation = 0

            if doc_type == "PARTS WORKSHEET" and not simplified["hasData"]:
                rotated = rotate_pdf_bytes(info["pdf"], ROTATION_FALLBACK_DEGREES)
                analyze2 = client.begin_analyze_document(
                    model_id,
                    body=rotated,
                    content_type="application/pdf"
                ).result()
                simplified2 = simplify_analyze_result(analyze2)
                if simplified2["hasData"]:
                    simplified = simplified2
                    used_rotation = ROTATION_FALLBACK_DEGREES

            key = safe_slug(doc_type).lower()
            merged[key] = simplified["fields"]
            flatten_for_row(key, simplified["fields"], flattened)

            picked_pages[doc_type] = info["pageNumber"]
            confidence[doc_type] = info["confidence"]

            extracted_docs.append({
                "docType": doc_type,
                "modelId": model_id,
                "page": info["pageNumber"],
                "rotationApplied": used_rotation,
                "fieldCount": len(simplified["fields"])
            })

        final_doc = {
            "id": job_id,
            "type": "bundle_result",
            "fileName": file_name,
            "createdAt": utc_now_iso(),
            "status": "completed",
            "bundleDiagnostics": {
                "totalPages": len(reader.pages),
                "failedPages": len(failures),
                "failures": failures
            },
            "pickedPages": picked_pages,
            "confidence": confidence,
            "flattened": flattened,
            "extractedDocs": extracted_docs,
            **merged
        }

        container.upsert_item(final_doc)

        # CLEANUP
        delete_prefix(uploads, f"{job_id}/")

    except Exception as e:
        container.upsert_item({
            "id": job_id,
            "type": "bundle_result",
            "status": "failed",
            "error": str(e),
            "failedAt": utc_now_iso()
        })


# =========================
# UI Endpoint
# =========================

@app.route(route="job/{jobId}/ui", methods=["GET"])
def get_job_ui(req: func.HttpRequest):

    job_id = req.route_params.get("jobId")
    if not job_id:
        return func.HttpResponse("Missing jobId", status_code=400)

    container = cosmos_container()

    try:
        doc = container.read_item(item=job_id, partition_key=job_id)
    except CosmosResourceNotFoundError:
        return func.HttpResponse("Not ready or invalid jobId", status_code=404)

    return func.HttpResponse(json.dumps(doc), mimetype="application/json")
