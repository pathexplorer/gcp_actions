from google.cloud import firestore
from datetime import datetime, timedelta
from gcp_actions import local_runner as lr

lr.check_cloud_or_local_run("/home/stas/Dropbox/projects/BigBikeData/keys.env")

db = firestore.Client()

def check_swith_status():
    # Read the state on every call (or cache it periodically)
    flag_doc = db.collection("config").document("feature_flags").get()

    if flag_doc.exists and flag_doc.to_dict().get("data_processing_enabled"):
        print("Mode is ON. Executing function.")
        return "prod"
    else:
        print("Mode is OFF. Skipping function.")
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
        True if message was ALREADY processed (duplicate)
        False if message is NEW (first time seeing it)
    """
    doc_ref = db.collection(collection_name).document(idempotency_key)
    
    try:
        # Use Firestore transaction to prevent race conditions
        from google.cloud.firestore import SERVER_TIMESTAMP
        
        doc = doc_ref.get()
        
        if doc.exists:
            # Message already processed
            processed_at = doc.to_dict().get('processed_at')
            print(f"⚠️ Duplicate message detected: {idempotency_key} (processed at {processed_at})")
            return True
        else:
            # First time seeing this message - mark it as processed
            doc_ref.set({
                'idempotency_key': idempotency_key,
                'processed_at': SERVER_TIMESTAMP,
                'expires_at': datetime.utcnow() + timedelta(hours=ttl_hours)
            })
            print(f"✅ New message: {idempotency_key} - marked as processed")
            return False
            
    except Exception as e:
        print(f"❌ Error checking idempotency: {e}")
        # On error, assume it's new to avoid losing messages
        # (better to process twice than not at all)
        return False


def mark_processing_complete(idempotency_key: str, collection_name: str = "processed_messages", result_data: dict = None):
    """
    Update the processed message record with completion data.
    
    Args:
        idempotency_key: Unique identifier for the message/operation
        collection_name: Firestore collection name
        result_data: Optional dictionary with processing results
    """
    doc_ref = db.collection(collection_name).document(idempotency_key)
    
    update_data = {
        'completed_at': firestore.SERVER_TIMESTAMP,
        'status': 'completed'
    }
    
    if result_data:
        update_data['result'] = result_data
    
    doc_ref.update(update_data)
    print(f"✅ Marked {idempotency_key} as completed")


def mark_processing_failed(idempotency_key: str, collection_name: str = "processed_messages", error_message: str = None):
    """
    Mark a message as failed (allows retry logic if needed).
    
    Args:
        idempotency_key: Unique identifier for the message/operation
        collection_name: Firestore collection name
        error_message: Error description
    """
    doc_ref = db.collection(collection_name).document(idempotency_key)
    
    doc_ref.update({
        'failed_at': firestore.SERVER_TIMESTAMP,
        'status': 'failed',
        'error': error_message or 'Unknown error'
    })
    print(f"❌ Marked {idempotency_key} as failed: {error_message}")


if __name__ == "__main__":
    # Test the switch status
    val = check_swith_status()
    print(val)
    
    # Test idempotency
    test_key = "test-message-123"
    
    print("\n--- Testing Idempotency ---")
    is_duplicate_1 = check_and_mark_processed(test_key)
    print(f"First call - Is duplicate? {is_duplicate_1}")  # Should be False
    
    is_duplicate_2 = check_and_mark_processed(test_key)
    print(f"Second call - Is duplicate? {is_duplicate_2}")  # Should be True
    
    mark_processing_complete(test_key, result_data={'files_processed': 1})
