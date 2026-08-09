"""
Google Cloud Storage blob manipulation utilities.

Provides filename generation, upload, download, and deletion operations
for GCS blobs with support for Requester Pays and local development.
"""

from gcp_actions.client import get_bucket
from google.api_core.exceptions import GoogleAPICallError, Forbidden
from typing import Any, Literal
import json
import logging
import os
import uuid

logger = logging.getLogger(__name__)


def generate_unique_filename(original_filename: str, subcatalog: str) -> str:
    """Generate a unique GCS object path using UUID4 while preserving the file extension."""
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
    """Handle GCS upload operations for files, filenames, and raw strings."""

    def __init__(
            self,
            bucket_name: str,
            gcs_path: str,
            local_path: Any | None = None,
            content_type_set: str | None = None,
    ) -> None:
        """Initialize the storage manipulation handler.

        Args:
            bucket_name: GCS bucket name or environment variable containing it.
            gcs_path: Destination path in GCS (e.g., 'folder/file.ext').
            local_path: Local file path or string content to upload.
            content_type_set: MIME type for string uploads (e.g., 'application/json').
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
    def _check_filename(self) -> None:
        """Verify that the local file exists before upload."""
        if not os.path.isfile(self.local_path):
            raise FileNotFoundError(f"Local file not found: {self.local_path}")

    def _upload_from_file(self) -> None:
        """Upload using file object (for large files)."""
        self._check_filename()
        self.set_blob.upload_from_file(self.local_path)

    def _upload_from_filename(self) -> None:
        """Upload using local file path."""
        self._check_filename()
        self.set_blob.upload_from_filename(self.local_path)

    # --- String ---
    def _upload_from_string(self) -> None:
        """Upload raw string content with explicit content type."""
        self.set_blob.upload_from_string(self.local_path, content_type=self.content_type_set)

    # --- Start upload
    FileType = Literal[
        "file",
        "filename",
        "string"
    ]

    def upload_to_gcp_bucket(
        self,
        filetype: FileType = "",
        user_project: str | None = None
    ) -> str | None:
        """Upload content to GCS based on the specified filetype mode."""
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
    """Download a blob from GCS, supporting both file and text (JSON) modes.

    Args:
        bucket_name: Bucket name or environment variable name.
        blob_name: Full path to the blob in the bucket.
        local_path: Local destination path (required for filetype='blob').
        filetype: 'blob' to save as file, 'text' to parse as JSON string.
        user_project: Project ID to bill for Requester Pays buckets.

    Returns:
        True for successful file download, parsed JSON dict for text mode,
        False if blob not found in blob mode, empty dict if not found in text mode.
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
    """Delete a GCS blob, returning True on success or if already absent.

    Handles permission errors, API failures, and missing blobs gracefully
    without raising exceptions. Logs detailed error context for debugging.

    Args:
        bucket_name: GCS bucket name or environment variable name.
        blob_name: Full path to the blob in the bucket.
        user_project: Project ID to bill for Requester Pays buckets.

    Returns:
        True if deleted or already missing, False on permission/API errors.
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
