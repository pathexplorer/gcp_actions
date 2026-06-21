import datetime
from gcp_actions.blob_manipulation import get_bucket
from gcp_actions.common_utils.local_runner import check_cloud_or_local_run
import os
from google.api_core import exceptions as google_exceptions

import logging
logger = logging.getLogger(__name__)

check_cloud_or_local_run()

def g_download_link(
    bucket_name: str, 
    blob_name: str, 
    expiration_minutes: int = 60,
    download_filename: str | None = None,
    impersonate_sa: str | None = None
) -> str:
    """
    Generates a temporary, secure download link for a private file,
    optionally specifying the download filename and a service account to impersonate.

    Raises:
        google_exceptions.NotFound: If the bucket or blob does not exist.
        google_exceptions.Forbidden: If there are permission issues.
        Exception: For any other unexpected errors during URL generation.
    """
    logger.debug(f"Attempting to generate signed URL for blob '{blob_name}' in bucket '{bucket_name}'...")
    try:   
        # Pass the service account to impersonate to the get_bucket function
        bucket = get_bucket(bucket_name, impersonate_sa=impersonate_sa)
        blob = bucket.blob(blob_name)

        if not blob.exists():
            raise google_exceptions.NotFound(f"Blob '{blob_name}' not found in bucket '{bucket_name}'.")
    
        # Prepare the content disposition header if a filename is provided
        disposition = None
        if download_filename:
            # This tells the browser to download the file with the specified name
            disposition = f'attachment; filename="{download_filename}"'
    
        # This will now raise an exception if it fails (e.g., due to permissions)
        signed_url = blob.generate_signed_url(
            version="v4",
            expiration=datetime.timedelta(minutes=expiration_minutes),
            method="GET",
            response_disposition=disposition
        )
        
        logger.debug(f"✅ Successfully generated signed URL for {blob_name}")
        return signed_url
    except google_exceptions.NotFound as e:
        logger.error(f"Resource not found for blob '{blob_name}' in bucket '{bucket_name}': {e}")
        raise
    except google_exceptions.Forbidden as e:
        logger.error(f"Permission denied for blob '{blob_name}'. Check IAM permissions for service account. Details: {e}")
        raise
    except Exception as e:
        logger.critical(f"An unexpected error occurred while generating signed URL for '{blob_name}': {e}")
        raise

if __name__ == "__main__":
    check_cloud_or_local_run()
    
    # --- Corrected Test Execution ---
    # 1. Get the bucket name from the environment variable.
    bucket_name = os.getenv("GCS_PUB_OUTPUT_BUCKET")
    if not bucket_name:
        raise EnvironmentError("GCS_PUB_OUTPUT_BUCKET environment variable not set.")
        
    # 2. Define the BLOB NAME (the path INSIDE the bucket), not the full gs:// URI.
    blob_name = "fit_clean/11a62171-4794-414c-9345-432220e12494_cleaned.fit"
    
    print(f"Bucket: {bucket_name}")
    print(f"Blob:   {blob_name}")
    
    # 3. Call the function with the correct parameters.
    try:
        signed_url = g_download_link(bucket_name, blob_name)
        print("\nGenerated Signed URL:")
        print(signed_url)
    except Exception as e:
        print(f"\nError generating link: {e}")
