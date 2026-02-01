
# todo REMOVE three functions
# def create_download_record(bucket_name: str, blob_name: str, download_filename: str, expiration_hours: int = 1) -> str:
#     db = get_any_client("firestore")
#     download_id = str(uuid.uuid4())
#     doc_ref = db.collection("download_links").document(download_id)
#     try:
#         doc_ref.set({'bucket_name': bucket_name,'blob_name': blob_name,'download_filename': download_filename,'created_at': firestore.SERVER_TIMESTAMP,'expires_at': datetime.utcnow() + timedelta(hours=expiration_hours)})
#         logger.debug(f"Created download record {download_id} for '{blob_name}'.")
#         return download_id
#     except Exception as e:
#         logger.error(f"Failed to create download record for '{blob_name}': {e}")
#         raise
# def mark_processing_complete(idempotency_key: str, collection_name: str = "processed_messages", result_data: dict = None):
#     db = get_any_client("firestore")
#     doc_ref = db.collection(collection_name).document(idempotency_key)
#     update_data = {'completed_at': firestore.SERVER_TIMESTAMP,'status': 'completed'}
#     if result_data:
#         update_data['result'] = result_data
#     doc_ref.update(update_data)
#     logger.info(f"✅ Marked {idempotency_key} as completed")
# def mark_processing_failed(idempotency_key: str, collection_name: str = "processed_messages", error_message: str = None):
#     db = get_any_client("firestore")
#     doc_ref = db.collection(collection_name).document(idempotency_key)
#     fail_update = {'failed_at': firestore.SERVER_TIMESTAMP,'status': 'failed','error': error_message or 'Unknown error'}
#     doc_ref.update(fail_update)
#     logger.error(f"❌ Marked {idempotency_key} as failed: {error_message}")