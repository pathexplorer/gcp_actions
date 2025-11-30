from typing import Any, Optional, Union
from google.api_core.exceptions import GoogleAPICallError, Forbidden
import uuid
from gcp_actions.client import get_bucket
import json
import logging
import os


logger = logging.getLogger(__name__)

# --- Private Helper (Not meant to be called directly) ---
def _get_blob_target(bucket_name: str, gcs_path: str, user_project: str | None = None):
    """
    Internal helper to avoid repeating connection logic.
    Returns the blob object ready for operation.
    """
    if not gcs_path:
        raise ValueError("GCS path must not be empty")

    bucket = get_bucket(bucket_name, user_project=user_project)
    return bucket.blob(gcs_path)


# --- Public Reusable Functions ---

def upload_file(
        bucket_name: str,
        local_path: str,
        gcs_path: str,
        user_project: str | None = None
) -> str:
    """
    Uploads a local file to GCS.
    Replaces: filetype="filename" or "file"
    """
    if not os.path.isfile(local_path):
        raise FileNotFoundError(f"Local file not found: {local_path}")

    try:
        blob = _get_blob_target(bucket_name, gcs_path, user_project)
        blob.upload_from_filename(local_path)
        # logger.debug(f"Uploaded file: {local_path} -> {gcs_path}")
        return gcs_path
    except Exception as e:
        raise RuntimeError(f"Failed to upload {local_path}: {e}")


def upload_json(
        bucket_name: str,
        data: Any,
        gcs_path: str,
        user_project: str | None = None
) -> str:
    """
    Uploads a Python dictionary/list as a JSON file.
    Replaces: filetype="string"
    """
    try:
        blob = _get_blob_target(bucket_name, gcs_path, user_project)
        blob.upload_from_string(
            json.dumps(data),
            content_type='application/json'
        )
        # logger.debug(f"Uploaded JSON to {gcs_path}")
        return gcs_path
    except Exception as e:
        raise RuntimeError(f"Failed to upload JSON: {e}")


def upload_content(
        bucket_name: str,
        content: str,
        gcs_path: str,
        content_type: str = "text/plain",
        user_project: str | None = None
) -> str:
    """
    Uploads raw string content (CSV string, text, etc).
    Replaces: filetype="string_path"
    """
    try:
        blob = _get_blob_target(bucket_name, gcs_path, user_project)
        blob.upload_from_string(content, content_type=content_type)
        return gcs_path
    except Exception as e:
        raise RuntimeError(f"Failed to upload content: {e}")


def download_file(
        bucket_name: str,
        gcs_path: str,
        local_path: str,
        user_project: str | None = None
) -> str:
    """Downloads a file from GCS to local disk."""
    try:
        blob = _get_blob_target(bucket_name, gcs_path, user_project)

        if not blob.exists():
            raise FileNotFoundError(f"GCS path not found: {gcs_path}")

        # Create a local directory if needed
        os.makedirs(os.path.dirname(local_path), exist_ok=True)

        blob.download_to_filename(local_path)
        return local_path
    except Exception as e:
        raise RuntimeError(f"Failed to download {gcs_path}: {e}")


def delete_gcs_file(
        bucket_name: str,
        gcs_path: str,
        user_project: str | None = None
) -> bool:
    """Deletes a file if it exists. Returns True if deleted or didn't exist."""
    try:
        blob = _get_blob_target(bucket_name, gcs_path, user_project)
        if blob.exists():
            blob.delete()
            return True
        return True  # Considered success if it's already gone
    except Exception as e:
        # Log error here
        return False





def generate_unique_filename(original_filename: str, subcatalog: str) -> str:
    """
    Generates a unique filename within a specified subcatalog.

    Args:
        original_filename: The original name of the file (e.g., "my_photo.jpg").
                           Must be a non-empty string.
        subcatalog: The subcatalog where the file will be stored (e.g., "user_uploads").
                    Must be a non-empty string.

    Returns:
        A unique path string in the format "subcatalog/uuid4.extension".

    Raises:
        ValueError: If original_filename or subcatalog are empty or invalid.
        Exception: For any other unexpected errors during generation.
    """
    try:
        if not original_filename or not isinstance(original_filename, str):
            raise ValueError("original_filename must be a non-empty string.")
        if not subcatalog or not isinstance(subcatalog, str):
            raise ValueError("subcatalog must be a non-empty string.")

        file_id = str(uuid.uuid4())
        _, file_extension = os.path.splitext(original_filename)

        # Sanitize subcatalog to remove leading/trailing slashes for clean path construction
        sane_subcatalog = subcatalog.strip('/')

        uniq_path = f"{sane_subcatalog}/{file_id}{file_extension}"
        logger.debug(f"Generated unique path: {uniq_path}")
        return uniq_path
    except Exception as e:
        logger.error(f"Failed to generate unique filename for '{original_filename}': {e}")
        raise