from typing import Any, Optional, Union
from gcp_actions.client import get_bucket
from google.api_core.exceptions import GoogleAPICallError, Forbidden
import json
import logging
import os
import uuid

# from google.cloud.storage.client import Client
logger = logging.getLogger(__name__)


class GCSClient:
    """
    A unified client for Google Cloud Storage operations (Upload, Download, Delete)
    for a single, specified bucket.
    Usage:
    # 1. Initialize the client (one-time per bucket/project)
        try:
            gcs_client = GCSClient(bucket_name=BUCKET_NAME, user_project="my-billing-project")
        except ValueError as e:
            print(f"Error: {e}")
    # Upload a local CSV file
        local_path = "/tmp/my_data.csv"
        gcs_path = "landing/2025/my_data.csv"
        try:
            uploaded_path = gcs_client.upload_from_filename(local_file, gcs_output)
            print(f"Uploaded: {uploaded_path}")
        except RuntimeError as e:
            print(f"Upload failed: {e}")

        # Download and parse a JSON configuration
        gcs_config_path = "config/settings.json"
        try:
            config_data = gcs_client.download_as_json(gcs_config_path)
            print(f"Downloaded config: {config_data['version']}")
        except FileNotFoundError:
            print("Config file not found.")

        # Delete a temporary file
        gcs_temp_file = "temp/junk.txt"
        if gcs_client.delete_blob(gcs_temp_file):
            print(f"Deleted {gcs_temp_file} successfully.")
    """

    def __init__(self, bucket_name: str, user_project: Optional[str] = None):
        """
        Initializes the GCS client and fetches the specified bucket.

        Args:
            bucket_name: The name of the GCS bucket.
            user_project: The project ID to bill for Requester Pays requests.
        """
        if not bucket_name:
            raise ValueError("Bucket name must be provided.")

        # Initialization client and fetch the bucket (REUSED LOGIC)
        # Assuming get_bucket is a function that returns a google.cloud.storage.Bucket object
        # logger.debug(f"Initializing GCSClient for bucket: {bucket_name}")
        self.bucket = get_bucket(bucket_name, user_project=user_project)
        self.bucket_name = bucket_name

    # --- Helper Methods ---

    def _get_blob(self, gcs_path: str):
        """Helper to validate a path and return the Blob object."""
        if not gcs_path:
            raise ValueError("GCS path must not be empty.")
        return self.bucket.blob(gcs_path)

    def _handle_error(self, operation: str, path: str, e: Exception):
        """Helper to log and re-raise operation errors."""
        error_msg = f"Failed to {operation} gs://{self.bucket_name}/{path}: {e}"
        # logger.error(error_msg)
        raise RuntimeError(error_msg) from e

    # --- UPLOAD Methods ---

    def upload_from_filename(self, local_path: str, gcs_path: str) -> str | None:
        """
        :param local_path: /folder/filename, on virtual machine
        :param gcs_path: folder/filename.extension, on Storage, gs://
        Uploads a file from a local path using the content of the file.
        (Original 'filetype' == 'filename' or 'file')
        """
        blob = self._get_blob(gcs_path)
        if not os.path.isfile(local_path):
            raise FileNotFoundError(f"Local file not found: {local_path}")

        try:
            # Note: upload_from_filename is typically preferred over upload_from_file
            # when the path is known, as it handles file opening/closing internally.
            blob.upload_from_filename(local_path)
            # logger.debug(f"Uploaded file in GCS: {gcs_path}")
            return gcs_path
        except Exception as e:
            self._handle_error("upload_from_filename", gcs_path, e)

    def upload_json(self, data: Any, gcs_path: str) -> str | None:
        """
        :param gcs_path: folder/filename.extension, on Storage, gs://
        :param data /folder/filename, on virtual machine
        Serializes a Python object to JSON and uploads it as a string.
        (Original 'filetype' == 'string')
        """
        blob = self._get_blob(gcs_path)
        try:
            json_string = json.dumps(data)
            blob.upload_from_string(json_string, content_type='application/json')
            # logger.debug(f"Uploaded JSON in GCS: {gcs_path}")
            return gcs_path
        except Exception as e:
            self._handle_error("upload_json", gcs_path, e)

    def upload_from_string(self, content_string: str, gcs_path: str, content_type: Optional[str] = None) -> str | None:
        """
        :param content_type:
        :param gcs_path: folder/filename.extension, on Storage, gs://
        :param content_string: /folder/filename, on virtual machine
        Uploads a raw string (e.g., text, CSV content).
        (Original 'filetype' == 'string_path')
        """
        blob = self._get_blob(gcs_path)
        try:
            blob.upload_from_string(content_string, content_type=content_type)
            # logger.debug(f"Uploaded raw string in GCS: {gcs_path}")
            return gcs_path
        except Exception as e:
            self._handle_error("upload_from_string", gcs_path, e)

    # --- DOWNLOAD Methods ---

    def download_to_filename(self, gcs_path: str, local_path: str) -> str | None:
        """
        Downloads a GCS blob to a local filename.
        (Original 'filetype' == 'blob')
        """
        if not local_path:
            raise ValueError("Local path must not be empty for file download.")

        blob = self._get_blob(gcs_path)
        if not blob.exists():
            raise FileNotFoundError(f"Blob not found in GCS: {gcs_path}")

        # Create a folder if it doesn't exist
        os.makedirs(os.path.dirname(local_path), exist_ok=True)

        try:
            blob.download_to_filename(local_path)
            # logger.debug(f"Successfully downloaded {gcs_path}")
            return local_path
        except Exception as e:
            self._handle_error(f"download_to_filename to {local_path}", gcs_path, e)

    def download_as_json(self, gcs_path: str) -> Any | None:
        """
        Downloads the blob content as a string and parses it as JSON.
        (Legacy 'filetype' == 'text')
        :param gcs_path: folder/filename.extension, on Storage, gs://
        local_path is not used here.
        """
        blob = self._get_blob(gcs_path)
        if not blob.exists():
            # The original code returned {}, changed to raise a more predictable error.
            raise FileNotFoundError(f"Blob not found in GCS: {gcs_path}")

        try:
            content = blob.download_as_text()
            return json.loads(content)
        except Exception as e:
            self._handle_error("download_as_json", gcs_path, e)

    # --- DELETE Method ---

    def delete_blob(self, gcs_path: str) -> bool:
        """
        Deletes a Google Cloud Storage blob robustly.
        """
        blob = self._get_blob(gcs_path)

        try:
            if blob.exists():
                logger.debug(f"Attempting deletion: gs://{self.bucket_name}/{gcs_path}")
                blob.delete()
                logger.debug(f"✅ Deletion successful: {gcs_path}")
                return True
            else:
                # If the blob doesn't exist, the goal (absence) is achieved.
                logger.debug(f"🟡 Blob not found (already absent): {gcs_path}")
                return True

        except Forbidden:
            # 403 Error: Permission Issue
            logger.error(f"❌ [403 Forbidden]: Cannot delete {gcs_path}. Check permissions.")
            return False

        except GoogleAPICallError as e:
            # Catch general API errors
            logger.error(f"❌ GCS API call failed for {gcs_path}. Details: {e}")
            return False

        except Exception as e:
            # Catch any unexpected Python exceptions
            logger.error(f"❌ UNEXPECTED during blob deletion for {gcs_path}: {e}")
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


