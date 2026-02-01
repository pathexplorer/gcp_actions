from gcp_actions.client import get_bucket
from google.api_core.exceptions import GoogleAPICallError, Forbidden
from typing import Any, Literal
import json
import logging
import os
import uuid

logger = logging.getLogger(__name__)

def generate_unique_filename(original_filename: str, subcatalog: str) -> str:
    """
    Generates a unique filename within a specified subcatalog.

    :param original_filename: The original name of the file (e.g., "my_photo.jpg").
    :param subcatalog: The subcatalog where the file will be stored (e.g., "user_uploads").
    :return: A unique path string in the format "subcatalog/uuid4.extension".
    :raise ValueError: If original_filename or subcatalog are empty or invalid.
    :raise Exception: For any other unexpected errors during generation.
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

# todo rewrite module as class
class StorageManipulations:
    def __init__(
            self,
            bucket_name: str,
            gcs_path: str,
            local_path: Any | None = None,
            content_type_set: str | None = None,
    ) -> None:
        """
        :param bucket_name: variable GCS_BUCKET_NAME or GCS_PUBLIC_BUCKET
        :param gcs_path: folder/filename.extension, on Storage, gs://
        :param local_path: /folder/filename, on a virtual machine
        :param content_type_set:
        """
        # --- Variables ---
        self.gcs_path = gcs_path
        self.bucket_name = bucket_name
        self.local_path = local_path
        self.content_type_set = content_type_set
        # --- Assigning --
        self.bucket = get_bucket(bucket_name)
        self.set_blob = self.bucket.blob(self.gcs_path)

    # --- Filename ---
    def _check_filename(self):
        if not os.path.isfile(self.local_path):
            raise FileNotFoundError(f"Local file not found: {self.local_path}")
    def _upload_from_file(self):
        self._check_filename()
        self.set_blob.upload_from_file(self.local_path)
    def _upload_from_filename(self):
        self._check_filename()
        self.set_blob.upload_from_filename(self.local_path)

    # --- String ---
    def _upload_from_string(self):
        self.set_blob.upload_from_string(self.local_path, content_type=self.content_type_set)

    # --- Start upload
    type FileType = Literal[
        "file",
        "filename",
        "string"
    ]
    def upload_to_gcp_bucket(
        self,
        filetype: FileType = "",
        user_project: str | None = None
    ) -> str | None:
        """
        :param user_project:
        :param filetype: "filename" or "string" (json)
        """
        if not self.gcs_path:
            raise ValueError("GCS path must not be empty")
        logger.debug("Start upload to GCS")

        try:
            if filetype == "file":
                self._upload_from_file()
            elif filetype == "filename":
                self._upload_from_filename()
            elif filetype == "string":
                self._upload_from_string()
            logger.debug(f"Uploaded file in GCS: {self.gcs_path}")
            return self.gcs_path
        except Exception as e:
            raise RuntimeError(f"Failed to upload {self.local_path} to {self.gcs_path}: {e}")


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
    bucket = get_bucket(bucket_name)
    if not blob_name:
        raise ValueError("Blob name must not be empty")
    if filetype not in ("blob", "text"):
        pass # return None if blob exists but filetype is invalid/missing
    blob = bucket.blob(blob_name)
    if not blob.exists():
        if filetype == "blob":
            return False
        elif filetype == "text":
            logger.info("Create empty text blob")
            return {}
    # 2. Handling Filetypes
    if filetype == "blob":
        if not local_path:
            # Re-introduce the mandatory check for 'blob' download
            raise ValueError("Local path must not be empty for filetype 'blob'")
        # Create a folder if it doesn't exist
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
        :param bucket_name: GCS bucket name
        :param blob_name: The full path to the blob inside the bucket (e.g., 'data/file.csv').
        :param user_project:
        :return: True if the blob is successfully deleted or if it did not exist. False on error.
        :exception Forbidden:
        :exception GoogleAPICallError:
        :exception Exception:
    """
    bucket = get_bucket(bucket_name)
    if not blob_name:
        logger.warning("Attempted to delete blob with empty name. Skipping.")
        return True  # Treat empty name as success (nothing to delete)

    blob = bucket.blob(blob_name)

    try:
        if blob.exists():
            logger.debug(f"Attempting deletion: gs://{bucket.name}/{blob_name}")
            blob.delete()
            logger.debug(f"✅ Deletion successful: {blob_name}")
            return True
        else:
            # If the blob doesn't exist, the goal (absence) is achieved.
            logger.debug(f"🟡 Blob not found (already absent): {blob_name}")
            return True

    except Forbidden:
        # 403 Error: Permission Issue
        logger.error(
            f"❌ [403 Forbidden]: Cannot delete {blob_name}. Check the service account's 'storage.objectAdmin' role.")
        return False

    except GoogleAPICallError as e:
        # Catch general API errors (e.g., network issues, timeouts, object lock)
        logger.error(f"❌ GCS API call failed for {blob_name}. Details: {e}")
        return False

    except Exception as e:
        # Catch any unexpected Python exceptions
        logger.error(f"❌ UNEXPECTED during blob deletion for {blob_name}: {e}")
        return False
# -----------------------
# Test sections
# -----------------------
if __name__ == "__main__":
    bucket_test = get_bucket("GCS_BUCKET_NAME")
    print(bucket_test)
    for blob_test in bucket_test.list_blobs():
        print("Files in bucket:", blob_test.name)
