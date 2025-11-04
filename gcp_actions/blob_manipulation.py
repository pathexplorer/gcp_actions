import os
import json
from typing import Any
from gcp_actions.client import get_bucket
import logging

# Get the root logger (which GCF has already configured)
logger = logging.getLogger()
# Set its level directly
logger.setLevel(logging.DEBUG)

bucket = get_bucket()

def upload_to_gcp_bucket(
        gcs_path: str,
        local_path: Any | None = None,
        filetype: str = ""
) -> None:
    """
    :param gcs_path: folder/filename.extension, on Storage, gs://
    :param local_path: /folder/filename, on virtual machine
    :param filetype: "filename" or "string" (json)
    :return:
    """
    if not gcs_path:
        raise ValueError("GCS path must not be empty")

    if filetype == "filename":
        if not os.path.isfile(local_path):
            raise FileNotFoundError(f"Local file not found: {local_path}")
        try:
            bucket.blob(gcs_path).upload_from_filename(local_path)
        except Exception as e:
            raise RuntimeError(f"Failed to upload {local_path} to {gcs_path}: {e}")
    if filetype == "string":
        blob = bucket.blob(gcs_path)
        try:
            blob.upload_from_string(json.dumps(local_path), content_type='application/json')
        except Exception as e:
            raise RuntimeError(f"Failed to upload {local_path} to {gcs_path}: {e}")


def download_from_gcp_bucket(
        blob_name: str,
        local_path: str | None = None,
        filetype: str = ""
) -> bool | Any | None:
    """
    :param blob_name: folder/filename.extension, on Storage, gs://
    :param local_path: /folder/filename, on virtual machine, ONLY for blob, not text
    :param filetype: "blob" (download_to_filename) or "text" (download_to_text).
    :return:
    """
    if not blob_name:
        raise ValueError("Blob name must not be empty")
    if filetype not in ("blob", "text"):
        pass # return None if blob exists but filetype is invalid/missing
    blob = bucket.blob(blob_name)
    if not blob.exists():
        if filetype == "blob":
            return False
        elif filetype == "text":
            logging.info("Create empty text blob")
            return {}
    # 2. Handling Filetypes
    if filetype == "blob":
        if not local_path:
            # Re-introduce the mandatory check for 'blob' download
            raise ValueError("Local path must not be empty for filetype 'blob'")
        # Create folder if it doesn't exist
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        try:
            blob.download_to_filename(local_path)
            return True
        except Exception as e:
            raise RuntimeError(f"Failed to download '{blob_name}' to '{local_path}': {e}")

    if filetype == "text":
        # local_path is optional and ignored here.
        try:
            content = blob.download_as_text()
            return json.loads(content)
        except Exception as e:
            # Use only blob_name in error, as local_path wasn't needed
            raise RuntimeError(f"Failed to download or parse JSON from '{blob_name}': {e}")

    return None


def delete_blob(blob_name):
    blob = bucket.blob(blob_name)
    if blob.exists():
        blob.delete()