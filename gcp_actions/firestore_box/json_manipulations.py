from datetime import datetime, timezone
from google.cloud.firestore_v1.field_path import FieldPath
from gcp_actions.client import get_any_client
from google.cloud import firestore
import logging, os
from typing import Dict, Any, Literal

logger = logging.getLogger(__name__)


class FirestoreMagic:
    def __init__(
            self,
            collection_name: str,
            doc_load_name: str,
            placeholder_full: dict | None = None
    ):
        """
        :param collection_name: name of the collection
        :param doc_load_name: single document name or path "doc/sub_collection/sub_doc_id"
        :param placeholder_full: dict with placeholder fields
        """
        self.client = get_any_client("firestore")
        self.collection_name = collection_name
        self.doc_load_name = doc_load_name
        self.placeholder_full = placeholder_full

    def _create_resource_if_not_exists(self, doc_ref):
        """Internal method to handle the 'if exist' logic."""
        if doc_ref is None:
               logger.error(f"An error occurred during resource creation: {doc_ref}, in None")
               return None
        doc_snapshot = doc_ref.get()
        if not doc_snapshot.exists:
            try:
                self.create_firejson(self.placeholder_full)
                logger.debug(f"Document '{self.doc_load_name}' not found. Creating new from template")
                # not sure it needs
                #return doc_ref.get()
            except Exception as e:
                logger.warning(f"An error occurred during resource creation: {e}")
                raise
        else:
            logger.debug("OK - Resource already exists")
            return doc_snapshot

    def load_firejson(self) -> dict[Any, Any] | None | Any:
        """
        If existed, get() + to_dict(). If not existed, create dict from a placeholder. \n
        :return: dict (which gets from snapshot) or empty dict if it doesn't exist
        """
        if "/" in self.doc_load_name:
            full_path = os.path.join(self.collection_name, self.doc_load_name)
            doc_ref = self.client.document(full_path)
            logger.debug(f"Detect path instead doc name, forming full path {full_path}")
        else:
            doc_ref = self.client.collection(self.collection_name).document(self.doc_load_name)
            logger.debug("Detect doc name")

        try:
            doc_snapshot = self._create_resource_if_not_exists(doc_ref)
            logger.debug(f"Doc dict {doc_snapshot}")
            doc_dict = doc_snapshot.to_dict()
            return doc_dict

        except LookupError as e:
            logger.error(f"Error loading doc from Firestore: {e}.")
        except Exception as e:
            logger.critical(f"Error loading doc from Firestore: {e}. Using defaults.")
            return {}

        # todo create separate access to prod and dev bases or config

        # The path uses the K_SERVICE name as the application identifier.
        # Path: /config/{APP_ID}/settings/data
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

    def create_firejson(self, data_name):
        """
        method set()
        :param data_name:
        :return:
        """
        doc_ref = self.client.collection(self.collection_name).document(self.doc_load_name)
        doc_ref.set(data_name)


    def set_firejson(self, data_name, merge: bool | None = None):
        """
        method set()
        :param data_name:
        :param merge: If None - overwrite a document or create it if it doesn't exist yet.
      If True - update fields in the document or create it if it doesn't exist
        :return:
        """
        doc_ref = self.client.collection(self.collection_name).document(self.doc_load_name)
        doc_ref.set(data_name, merge)

    def backup_document(self, backup_collection_name: str | None = None):
        """
        Copy the current document to a timestamped backup document.
        Writes to `{collection_name}_backups/{doc_name}_{ISO8601_timestamp}`.

        :param backup_collection_name: override backup collection (default: {collection}_backups)
        """
        if backup_collection_name is None:
            backup_collection_name = f"{self.collection_name}_backups"

        # Resolve source document reference (same logic as load_firejson)
        if "/" in self.doc_load_name:
            full_path = os.path.join(self.collection_name, self.doc_load_name)
            doc_ref = self.client.document(full_path)
        else:
            doc_ref = self.client.collection(self.collection_name).document(self.doc_load_name)

        doc_snapshot = doc_ref.get()
        if not doc_snapshot.exists:
            logger.warning("backup_document: source '%s/%s' does not exist — nothing to back up.",
                           self.collection_name, self.doc_load_name)
            return

        data = doc_snapshot.to_dict()
        if data is None:
            logger.warning("backup_document: source '%s/%s' is empty — nothing to back up.",
                           self.collection_name, self.doc_load_name)
            return

        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%S")
        backup_doc_id = f"{self.doc_load_name}_{timestamp}"
        backup_ref = self.client.collection(backup_collection_name).document(backup_doc_id)
        backup_ref.set(data)
        logger.info("Backup created: '%s/%s' (%d top-level keys).",
                    backup_collection_name, backup_doc_id, len(data))

    def prune_old_backups(self, max_backups: int = 30, backup_collection_name: str | None = None):
        """
        Keep only the `max_backups` most recent backup documents for this doc.
        Deletes older backups from the backup collection.

        :param max_backups: number of most recent backups to retain
        :param backup_collection_name: override backup collection (default: {collection}_backups)
        """
        if backup_collection_name is None:
            backup_collection_name = f"{self.collection_name}_backups"

        prefix = f"{self.doc_load_name}_"
        backups_ref = self.client.collection(backup_collection_name)

        # Stream all docs in the backup collection and filter by ID prefix
        try:
            backups = [snap for snap in backups_ref.stream() if snap.id.startswith(prefix)]
        except Exception as e:
            logger.warning("prune_old_backups: could not list backups: %s", e)
            return

        if not backups:
            logger.debug("prune_old_backups: no backups found with prefix '%s'.", prefix)
            return

        # Sort by document ID (contains ISO timestamp), oldest first
        backups.sort(key=lambda snap: snap.id)

        excess = len(backups) - max_backups
        if excess <= 0:
            logger.debug("prune_old_backups: %d backups, limit %d — nothing to prune.",
                         len(backups), max_backups)
            return

        for snap in backups[:excess]:
            snap.reference.delete()
            logger.debug("Pruned old backup: '%s/%s'.", backup_collection_name, snap.id)

        logger.info("prune_old_backups: deleted %d old backups, %d remain.",
                    excess, len(backups) - excess)

    def update_firejson(self, data_name):
        """
        method update()
        :param data_name:
        :return:
        """
        doc_ref = self.client.collection(self.collection_name).document(self.doc_load_name)
        doc_ref.update(data_name)


    Mode = Literal[
        "normal",
        "path"
    ]
    def delete_field_firejson(self, field_for_delete: str, mode: Mode = "normal"):
        """
        :param field_for_delete:
        :param mode:
        :return:
        """
        try:
            doc_ref = self.client.collection(self.collection_name).document(self.doc_load_name)
            if mode == "normal":
                doc_ref.update({field_for_delete: firestore.DELETE_FIELD})
                logger.info(f"Field '{field_for_delete}' successfully deleted from document '{self.doc_load_name}'.")
            elif mode == "path":
                field_for_delete = field_for_delete.strip()
                field_path = FieldPath(field_for_delete)

                doc_ref.update({field_path: firestore.DELETE_FIELD})
                logger.debug("Convert field name to path")
        except Exception as e:
            logger.warning(f"Field '{field_for_delete}' not found in document '{self.doc_load_name}'. No action taken. Error: {e}")



    def unpack_array_to_csv_string(
            self,
            document: Dict[str, Any],
            array_field_key: str,
            separator: str = ','
    ) -> str:
        """
        Unpacks a specific array field from a Firestore document into a
        single string with elements separated by the specified separator.

        :param document: The Firestore document dictionary (e.g., {"id": 1, "tags": ["a", "b"]}).
        :param array_field_key: The key of the array field to unpack (e.g., "tags").
        :param separator: The character to use for separation (default is comma ',').
        :returns: A comma-separated string of the array elements. Returns an empty string if the key is not found or the value is not a list.
        :rtype: string
        """

        # 1. Necessary checks and error handling (as requested)
        if not isinstance(document, dict):
            # Using a more specific exception for clarity
            raise TypeError("Input 'document' must be a dictionary.")

        # 2. Extract the array
        array_data: Any = document.get(array_field_key)

        # 3. Validation: Ensure the data is a list of strings
        if not isinstance(array_data, list):
            # Log a warning or handle as per project requirements
            print(f"Warning: Field '{array_field_key}' is missing or not a list. Returning empty string.")
            return ""

        # 4. Check if all elements are strings (optional, but good practice for join)
        if not all(isinstance(item, str) for item in array_data):
            # Convert non-string items to string defensively
            array_data = [str(item) for item in array_data]

        # 5. str.join() for efficient flattening
        return separator.join(array_data)

        # if doc.exists:
        #     loaded_data = doc.to_dict()
        #     logger.warning(f"{self.doc_name} already exists in Firestore")
        #     pass
        # else:
        #     try:
        #         self.doc_ref.set(base_json_data)
        #         logger.info(f"Created {self.doc_name}")
        #     except Exception as e:
        #         logger.error(f"Failed to create  record for '{base_json_data}': {e}")
        #         raise

    # def create_mass_json(self):
    #
    #     for doc_name, doc_json in zip(docs, docs_json):
    #         create_single_firejson(collection, doc_name, doc_json)
    #
    # def read_firejson(self):
    #     doc = self.doc_ref.get()
    #     if doc.exists:
    #         return doc.to_dict()
    #     else:
    #
    #         return None
    #


# if __name__ == "__main__":
#     ff = FirestoreMagic("bikes", "models")
#     print(ff.load_firejson())

#     from gcp_actions.common_utils.handle_logs import run_handle_logs
#
#     run_handle_logs()
#     print(os.environ.get("GCP_PROJECT_ID"))
#     print(os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"))
#     fs = FirestoreMagic( "telegram", "cursor")
#     logger.info(fs)
#     load_firejson = fs.load_firejson()
#     print(load_firejson)
# print(fs.unpack_array_to_csv_string(load_firejson, "word"))


#     from gcp_actions.common_utils.init_config import load_and_inject_config
#
#     list_of_secret_env_vars = ["APP_JSON_KEYS", "SEC_DROPBOX"]
#     list_of_sa_env_vars = [None, "S_ACCOUNT_DROPBOX"]
#     load_and_inject_config(list_of_secret_env_vars, list_of_sa_env_vars)
#     logger.debug("Configuration loaded successfully.")

#     # For creating heatmap settings from zero
#     collection = "heatmap"
#     docs = ["specs", "gravel_index", "mtb_index", "unknown_index"]
#     docs_json = [
#         {
#             "gravel": {
#                 "main_blob_name": "heatmap/gravel_v00.gpx",
#                 "compose_count": 0,
#                 "version": 0,
#             },
#             "mtb": {
#                 "main_blob_name": "heatmap/mtb_v00.gpx",
#                 "compose_count": 0,
#                 "version": 0,
#             },
#             "unknown": {
#                 "main_blob_name": "heatmap/unknown_v00.gpx",
#                 "compose_count": 0,
#                 "version": 0,
#             },
#         },
#         {},
#         {},
#         {}
#     ]
#
