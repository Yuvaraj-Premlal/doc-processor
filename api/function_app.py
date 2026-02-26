import azure.functions as func
import json
import os
import uuid
import base64
from azure.storage.blob import BlobServiceClient
from azure.storage.queue import QueueClient

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

def blob_service():
    return BlobServiceClient.from_connection_string(os.environ["AzureWebJobsStorage"])

def queue_client():
    # Uses same storage account as AzureWebJobsStorage
    conn = os.environ["AzureWebJobsStorage"]
    qname = os.environ.get("JOBS_QUEUE", "jobs")
    qc = QueueClient.from_connection_string(conn, qname)
    try:
        qc.create_queue()
    except Exception:
        pass
    return qc

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
        container_name = os.environ.get("UPLOADS_CONTAINER", "uploads")

        bsc = blob_service()
        container = bsc.get_container_client(container_name)
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

            msg = {"jobId": job_id, "pdfBlobPath": blob_path, "fileName": filename}
            # Queue messages are base64-encoded by SDK in many cases; safest is to send plain JSON string.
            qc.send_message(json.dumps(msg))
            enqueued.append(msg)

        return func.HttpResponse(
            json.dumps({"jobId": job_id, "files": uploaded, "queued": enqueued}),
            mimetype="application/json"
        )

    except Exception as e:
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            status_code=500,
            mimetype="application/json"
        )

# --- Worker (Queue Trigger) ---
@app.function_name(name="job_worker")
@app.queue_trigger(arg_name="msg", queue_name="%JOBS_QUEUE%", connection="AzureWebJobsStorage")
def job_worker(msg: func.QueueMessage) -> None:
    # For Step 10 v1: just log the message and write a status marker to results/<jobId>/status.json
    raw = msg.get_body().decode("utf-8")

    try:
        payload = json.loads(raw)
    except Exception:
        payload = {"raw": raw}

    job_id = payload.get("jobId", "unknown")
    pdf_blob = payload.get("pdfBlobPath", "")

    print(f"[worker] Received jobId={job_id} pdfBlobPath={pdf_blob}")

    # Write a simple status file so we can confirm async pipeline end-to-end
    results_container = os.environ.get("RESULTS_CONTAINER", "results")
    bsc = blob_service()
    rc = bsc.get_container_client(results_container)
    try:
        rc.create_container()
    except Exception:
        pass

    status_blob = f"{job_id}/status.json"
    status = {
        "jobId": job_id,
        "state": "queued_received",
        "pdfBlobPath": pdf_blob
    }
    rc.upload_blob(status_blob, json.dumps(status), overwrite=True)
    print(f"[worker] Wrote {results_container}/{status_blob}")
