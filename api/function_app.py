import azure.functions as func
import json
import os
import uuid
import base64
from datetime import datetime, timezone

from azure.storage.blob import BlobServiceClient
from azure.storage.queue import QueueClient

from azure.core.credentials import AzureKeyCredential
from azure.ai.documentintelligence import DocumentIntelligenceClient


app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)


# =========================
# Helpers
# =========================

def utc_now_iso():
    return datetime.now(timezone.utc).isoformat()


def blob_service():
    return BlobServiceClient.from_connection_string(os.environ["AzureWebJobsStorage"])


def queue_client():
    conn = os.environ["AzureWebJobsStorage"]
    qname = "jobs"  # hardcoded to avoid extra config
    qc = QueueClient.from_connection_string(conn, qname)
    try:
        qc.create_queue()
    except Exception:
        pass
    return qc


def di_client():
    endpoint = os.environ["DI_ENDPOINT"]
    key = os.environ["DI_KEY"]
    return DocumentIntelligenceClient(
        endpoint=endpoint,
        credential=AzureKeyCredential(key)
    )


def _jsonable(obj):
    """Convert SDK objects to JSON-serializable structure."""
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
                mimetype="application/json"
            )

        files = req.files.getlist("files")
        if not files:
            return func.HttpResponse(
                json.dumps({"error": "No files uploaded. Use field name 'files'."}),
                status_code=400,
                mimetype="application/json"
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

            container.upload_blob(blob_path, f.stream, overwrite=True)
            uploaded.append({"file": filename, "blob": blob_path})

            msg = {
                "jobId": job_id,
                "pdfBlobPath": blob_path,
                "fileName": filename
            }

            qc.send_message(json.dumps(msg))
            enqueued.append(msg)

        return func.HttpResponse(
            json.dumps({
                "jobId": job_id,
                "files": uploaded,
                "queued": enqueued
            }),
            mimetype="application/json"
        )

    except Exception as e:
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            status_code=500,
            mimetype="application/json"
        )


# =========================
# Queue Worker
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

    def write_status(stage, extra=None):
        status = {
            "jobId": job_id,
            "stage": stage,
            "pdfBlobPath": pdf_blob_path,
            "updatedAt": utc_now_iso()
        }
        if extra:
            status.update(extra)

        rc.upload_blob(status_blob, json.dumps(status), overwrite=True)
        print(f"[worker] status => {stage}")

    try:
        # 1️⃣ Mark as classifying
        write_status("classifying")

        # 2️⃣ Download PDF
        if not pdf_blob_path:
            raise ValueError("pdfBlobPath missing in queue payload.")

        pdf_bytes = uc.get_blob_client(pdf_blob_path).download_blob().readall()
        print(f"[worker] Downloaded PDF size: {len(pdf_bytes)} bytes")

        # 3️⃣ Call Azure Document Intelligence classifier
        classifier_id = "CEVA_Document_Classification"
        client = di_client()

        poller = client.begin_classify_document(
            classifier_id=classifier_id,
            classify_request=pdf_bytes
        )

        result = poller.result()

        # 4️⃣ Simplify results
        simplified_docs = []
        documents = getattr(result, "documents", []) or []

        for d in documents:
            doc_type = getattr(d, "doc_type", None) or getattr(d, "docType", None)
            confidence = getattr(d, "confidence", None)

            pages = []
            brs = getattr(d, "bounding_regions", None) or getattr(d, "boundingRegions", None) or []
            for br in brs:
                page_number = getattr(br, "page_number", None) or getattr(br, "pageNumber", None)
                if page_number:
                    pages.append(page_number)

            pages = sorted(set(pages)) if pages else None

            simplified_docs.append({
                "docType": doc_type,
                "confidence": confidence,
                "pages": pages
            })

        # 5️⃣ Save classification.json
        classification_blob = f"{job_id}/classification.json"
        classification_payload = {
            "jobId": job_id,
            "classifierId": classifier_id,
            "createdAt": utc_now_iso(),
            "simplified": simplified_docs,
            "raw": _jsonable(result)
        }

        rc.upload_blob(classification_blob, json.dumps(classification_payload), overwrite=True)

        # 6️⃣ Mark as classified
        write_status("classified", {
            "documentsFound": len(simplified_docs)
        })

        print(f"[worker] Classification complete for job {job_id}")

    except Exception as e:
        print(f"[worker][ERROR] {str(e)}")
        write_status("failed", {"error": str(e)})
        return
