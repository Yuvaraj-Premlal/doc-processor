import azure.functions as func
import json
import os
import uuid
from azure.storage.blob import BlobServiceClient

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

def blob_service():
    # Uses the Function runtime storage connection string by default.
    # Later we can switch to a dedicated DOCSTORE_CONNECTION if you want.
    return BlobServiceClient.from_connection_string(os.environ["AzureWebJobsStorage"])

@app.route(route="upload", methods=["POST"])
def upload(req: func.HttpRequest) -> func.HttpResponse:
    try:
        # NOTE: Azure Functions HttpRequest supports files when multipart/form-data is sent.
        # If this is called from a browser GET, it won't have files.
        if not hasattr(req, "files") or req.files is None:
            return func.HttpResponse(
                json.dumps({
                    "error": "No files found. Send multipart/form-data with field name 'files'."
                }),
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

        uploaded = []
        for f in files:
            filename = getattr(f, "filename", "file.pdf")
            blob_path = f"{job_id}/original/{filename}"
            # f.stream is a file-like object
            container.upload_blob(blob_path, f.stream, overwrite=True)
            uploaded.append({"file": filename, "blob": blob_path})

        return func.HttpResponse(
            json.dumps({"jobId": job_id, "files": uploaded}),
            mimetype="application/json"
        )

    except Exception as e:
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            status_code=500,
            mimetype="application/json"
        )
