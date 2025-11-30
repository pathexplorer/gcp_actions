from google.cloud import firestore
from datetime import datetime, timedelta
from gcp_actions.common_utils import local_runner as lr
from gcp_actions.client import get_any_client
import logging
import os
import uuid

logger = logging.getLogger(__name__)

# Get the APP_ID once at startup.
APP_ID = lr.check_cloud_or_local_run()
logger.info(f"Loading shared config for APP_ID: {APP_ID}")

CONFIG_NAMESPACE = os.environ.get('CONFIG_NAMESPACE', 'public')

# The 'db' client is no longer initialized here.
# It will be initialized inside each function to ensure it's always fresh.

def check_swith_status():
    """Checks a feature flag in Firestore. Fetches a fresh client on each call."""
    db = get_any_client("firestore")
    flag_doc = db.collection("config").document("feature_flags").get()

    if flag_doc.exists and flag_doc.to_dict().get("data_processing_enabled"):
        logger.debug("Mode is ON. Executing function.")
        return "prod"
    else:
        logger.debug("Mode is OFF. Skipping function.")
        return "testing"


def check_and_mark_processed(idempotency_key: str, collection_name: str = "processed_messages", ttl_hours: int = 24) -> bool:
    """
    Check if a message with the given idempotency key has already been processed.
    If not, mark it as processed.

    Args:
        idempotency_key: Unique identifier for the message/operation
        collection_name: Firestore collection to store processed message IDs
        ttl_hours: How long to keep the record (for cleanup)

    Returns:
        True if the message was ALREADY processed (duplicate)
        False if a message is NEW (first time seeing it)
    """
    db = get_any_client("firestore")
    doc_ref = db.collection(collection_name).document(idempotency_key)
    
    try:
        from google.cloud.firestore import SERVER_TIMESTAMP
        
        doc = doc_ref.get()
        
        if doc.exists:
            processed_at = doc.to_dict().get('processed_at')
            logger.warning(f"Duplicate message detected: {idempotency_key} (processed at {processed_at})")
            logger.warning(f"Check acknowledgement deadline in PubSub")
            return True
        else:
            doc_ref.set({
                'idempotency_key': idempotency_key,
                'processed_at': SERVER_TIMESTAMP,
                'expires_at': datetime.utcnow() + timedelta(hours=ttl_hours)
            })
            logger.debug(f"✅ New message: {idempotency_key} - marked as processed")
            return False
            
    except Exception as e:
        logger.error(f"❌ Error checking idempotency: {e}")
        return False


def create_download_record(bucket_name: str, blob_name: str, download_filename: str, expiration_hours: int = 1) -> str:
    """
    Creates a record in Firestore for a future download and returns a unique ID.

    Args:
        bucket_name: The GCS bucket of the file.
        blob_name: The GCS path of the file.
        download_filename: The desired filename for the user.
        expiration_hours: How long the link should be valid.

    Returns:
        The unique ID for this download record.
    """
    db = get_any_client("firestore")
    download_id = str(uuid.uuid4())
    doc_ref = db.collection("download_links").document(download_id)

    try:
        doc_ref.set({
            'bucket_name': bucket_name,
            'blob_name': blob_name,
            'download_filename': download_filename,
            'created_at': firestore.SERVER_TIMESTAMP,
            'expires_at': datetime.utcnow() + timedelta(hours=expiration_hours)
        })
        logger.debug(f"Created download record {download_id} for '{blob_name}'.")
        return download_id
    except Exception as e:
        logger.error(f"Failed to create download record for '{blob_name}': {e}")
        raise


def mark_processing_complete(idempotency_key: str, collection_name: str = "processed_messages", result_data: dict = None):
    """
    Update the processed message record with completion data.

    Args:
        idempotency_key: Unique identifier for the message/operation
        collection_name: Firestore collection name
        result_data: Optional dictionary with processing results
    """
    db = get_any_client("firestore")
    doc_ref = db.collection(collection_name).document(idempotency_key)
    
    update_data = {
        'completed_at': firestore.SERVER_TIMESTAMP,
        'status': 'completed'
    }
    
    if result_data:
        update_data['result'] = result_data
    
    doc_ref.update(update_data)
    logger.info(f"✅ Marked {idempotency_key} as completed")


def mark_processing_failed(idempotency_key: str, collection_name: str = "processed_messages", error_message: str = None):
    """
    Mark a message as failed (allows retry logic if needed).

    Args:
        idempotency_key: Unique identifier for the message/operation
        collection_name: Firestore collection name
        error_message: Error description
    """
    db = get_any_client("firestore")
    doc_ref = db.collection(collection_name).document(idempotency_key)
    
    doc_ref.update({
        'failed_at': firestore.SERVER_TIMESTAMP,
        'status': 'failed',
        'error': error_message or 'Unknown error'
    })
    logger.error(f"❌ Marked {idempotency_key} as failed: {error_message}")



def load_config_from_firestore() -> dict:
    """
    Fetches non-critical configuration from a Firestore document.
    The path uses the K_SERVICE name as the application identifier.
    Path: /config/{APP_ID}/settings/data
    """
    db = get_any_client("firestore")
    config_path = "config/local/settings/data"
    # --- END STATIC CONFIG PATH ---

    #todo create separate access to prod and dev bases or config

    # # --- DYNAMIC CONFIG PATH GENERATION ---
    # if APP_ID == 'local-dev-mode':
    #     # Local Development Path: Use a simple, global path that doesn't depend on K_SERVICE
    #
    #     config_path = f"config/local/settings/data"
    #     logger.warning(f"Local config path used (override via CONFIG_NAMESPACE): {config_path}")
    # else:
    #     # Production Path: Segregated by service name for multiservice environments
    #     logger.info(f"Firestore connected", APP_ID)
    #
    #     config_path = f"config/{APP_ID}/settings/data"
    # # --- END DYNAMIC CONFIG PATH ---

    try:
        # Define the path to the global application settings
        config_ref = db.document(config_path)
        doc = config_ref.get()

        if doc.exists:
            config_dict = doc.to_dict()
            logger.debug(f"Configuration loaded from Firestore path: {config_path}")
            return config_dict if config_dict else {}
        else:
            logger.warning(f"Firestore configuration document missing at: {config_path}. Using defaults.")
            return {}

    except Exception as e:
        logger.error(f"CRITICAL: Error loading config from Firestore: {e}. Using defaults.")
        # raise # Uncomment this line if failure to load config should prevent startup
        return {}

    # Load configuration on startup and store it globally

    # Example usage:
    # fire_fire = load_config_from_firestore()
    # timeout = fire_fire.get("DATABASE_TIMEOUT_SEC", 60)

if __name__ == "__main__":
    # Test the switch status
    # val = check_swith_status()
    # logger.info(val)

    load_config_from_firestore()
    #
    # # Test idempotency
    # test_key = "test-message-123"
    #
    # logger.info("\n--- Testing Idempotency ---")
    # is_duplicate_1 = check_and_mark_processed(test_key)
    # logger.info(f"First call - Is duplicate? {is_duplicate_1}") # Should be False
    #
    # is_duplicate_2 = check_and_mark_processed(test_key)
    # logger.info(f"Second call - Is duplicate? {is_duplicate_2}") # Should be True
    #
    # mark_processing_complete(test_key, result_data={'files_processed': 1})
