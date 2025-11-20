import os
import json
from typing import Any
from gcp_actions.client import get_bucket, get_env_and_cashed_it
import logging
from google.api_core.exceptions import GoogleAPICallError, Forbidden
import uuid
import local_runner as lr

lr.check_cloud_or_local_run()

# Get the root logger (which GCF has already configured)
logger = logging.getLogger()
# Set its level directly
logger.setLevel(logging.DEBUG)

def generate_unique_filename(original_filename, subcatalog: str):
    """
    :param original_filename:
    :param subcatalog: set name for subcatalog
    :return: "subcatalog/file.extension"
    """
    file_id = str(uuid.uuid4())
    print(f"file_id", file_id)
    _, file_extension = os.path.splitext(original_filename)
    uniq_path = f"{subcatalog}/{file_id}{file_extension}"
    print("uniq",uniq_path)
    return uniq_path

def upload_to_gcp_bucket(
        bucket_name: str,
        gcs_path: str,
        local_path: Any | None = None,
        filetype: str = "",
        content_type_set: str | None = None,
        user_project: str | None = None
) -> str | None:
    """
    :param user_project:
    :param content_type_set:
    :param bucket_name: variable GCS_BUCKET_NAME or GCS_PUBLIC_BUCKET
    :param gcs_path: folder/filename.extension, on Storage, gs://
    :param local_path: /folder/filename, on virtual machine
    :param filetype: "filename" or "string" (json)
    :return:
    """
    bucket = get_bucket(bucket_name, user_project=user_project)
    print("Use bucket:", bucket)
    if not gcs_path:
        raise ValueError("GCS path must not be empty")
    print("Start upload to GCS")
    if filetype == "filename":
        if not os.path.isfile(local_path):
            raise FileNotFoundError(f"Local file not found: {local_path}")
        try:
            bucket.blob(gcs_path).upload_from_filename(local_path)
            logging.info(f"Uploaded file in GCS: {gcs_path}")
            return gcs_path
        except Exception as e:
            raise RuntimeError(f"Failed to upload {local_path} to {gcs_path}: {e}")
    if filetype == "file":
        if not os.path.isfile(local_path):
            raise FileNotFoundError(f"Local file not found: {local_path}")
        try:
            bucket.blob(gcs_path).upload_from_file(local_path)
            logging.info(f"Uploaded file in GCS: {gcs_path}")
            return gcs_path
        except Exception as e:
            raise RuntimeError(f"Failed to upload {local_path} to {gcs_path}: {e}")
    if filetype == "string":
        blob = bucket.blob(gcs_path)
        try:
            blob.upload_from_string(json.dumps(local_path), content_type='application/json')
            logging.info(f"Uploaded JSON in GCS: {gcs_path}")
            return gcs_path
        except Exception as e:
            raise RuntimeError(f"Failed to upload {local_path} to {gcs_path}: {e}")
    if filetype == "string_path":
        blob = bucket.blob(gcs_path)
        try:
            blob.upload_from_string(local_path,content_type=content_type_set)
            logging.info(f"Uploaded in GCS: {gcs_path}")
            return gcs_path
        except Exception as e:
            raise RuntimeError(f"Failed to upload {local_path} to {gcs_path}: {e}")
    return None


def download_from_gcp_bucket(
        bucket_name: str,
        blob_name: str,
        local_path: str | None = None,
        filetype: str = "",
        user_project: str | None = None
) -> bool | Any | None:
    """
    Downloads a file from GCS, supporting Requester Pays.

    :param bucket_name: An env var name (e.g., "GCS_BUCKET_NAME") or the literal bucket name.
    :param blob_name: The full path to the blob inside the bucket.
    :param local_path: The local path to save the file (required for 'blob' filetype).
    :param filetype: "blob" to download to a file, or "text" to download as a string.
    :param user_project: The project ID to bill for Requester Pays requests.
    :return: Varies based on filetype.
    """
    bucket = get_bucket(bucket_name, user_project=user_project)
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

def delete_blob(
        bucket_name: str,
        blob_name: str,
        user_project: str | None = None

) -> bool:
    """
    Deletes a Google Cloud Storage blob robustly, handling existence,
    permissions, and API errors.
    Args:
        :param bucket_name: GCS bucket name
        :param blob_name: The full path to the blob inside the bucket (e.g., 'data/file.csv').
        :param user_project:
    Returns:
        True if the blob is successfully deleted or if it did not exist. False on error.

    """
    bucket = get_bucket(bucket_name, user_project=user_project)
    if not blob_name:
        logging.warning("🟡 WARNING: Attempted to delete blob with empty name. Skipping.")
        return True  # Treat empty name as success (nothing to delete)

    blob = bucket.blob(blob_name)

    try:
        if blob.exists():
            logging.info(f"🗑️ Attempting deletion: gs://{bucket.name}/{blob_name}")
            blob.delete()
            logging.info(f"✅ Deletion successful: {blob_name}")
            return True
        else:
            # If the blob doesn't exist, the goal (absence) is achieved.
            logging.info(f"🟡 Blob not found (already absent): {blob_name}")
            return True

    except Forbidden:
        # 403 Error: Permission Issue
        logging.error(
            f"❌ ERROR [403 Forbidden]: Cannot delete {blob_name}. Check the service account's 'storage.objectAdmin' role.")
        return False

    except GoogleAPICallError as e:
        # Catch general API errors (e.g., network issues, timeouts, object lock)
        logging.error(f"❌ ERROR: GCS API call failed for {blob_name}. Details: {e}")
        return False

    except Exception as e:
        # Catch any unexpected Python exceptions
        logging.error(f"❌ UNEXPECTED ERROR during blob deletion for {blob_name}: {e}")
        return False
# -----------------------
# Test sections
# -----------------------
if __name__ == "__main__":
    bucket_test = get_bucket("GCS_BUCKET_NAME")
    print(bucket_test)
    for blob_test in bucket_test.list_blobs():
        print("Files in bucket:", blob_test.name)

