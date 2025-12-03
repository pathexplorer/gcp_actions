from gcp_actions.client import get_any_client
import logging
logger = logging.getLogger(__name__)

class FirestoreMagic:
    def __init__(self, type_of_client, collection_name: str | list, doc_name: str | list, doc_json: dict | list):
        """
        :param type_of_client: "firestore" or "storage"
        """
        self.client = get_any_client(type_of_client)
        self.collection_name = collection_name
        self.doc_name = doc_name
        self.doc_json = doc_json
        self.doc_ref = self.client.collection(self.collection_name).document(self.doc_name)


    def create_single_firejson(self, base_json_data):
        """
        Create a new document in Firestore if it doesn't exist.
        :param collection_name:
        :param doc_name:
        :param base_json_data: template for create if not exist
        :return:
        """
        doc = self.doc_ref.get()
        if doc.exists:
            logger.warning(f"{self.doc_name} already exists in Firestore")
            pass
        else:
            try:
                self.doc_ref.set(base_json_data)
                logger.info(f"Created {self.doc_name}")
            except Exception as e:
                logger.error(f"Failed to create  record for '{base_json_data}': {e}")
                raise

    def create_mass_json(self):

        for doc_name, doc_json in zip(docs, docs_json):
            create_single_firejson(collection, doc_name, doc_json)

    def read_firejson(self):
        doc = self.doc_ref.get()
        if doc.exists:
            return doc.to_dict()
        else:

            return None



if __name__ == "__main__":
    from gcp_actions.common_utils.init_config import load_and_inject_config

    list_of_secret_env_vars = ["APP_JSON_KEYS", "SEC_DROPBOX"]
    list_of_sa_env_vars = [None, "S_ACCOUNT_DROPBOX"]
    load_and_inject_config(list_of_secret_env_vars, list_of_sa_env_vars)
    logger.debug("Configuration loaded successfully.")

    # For creating heatmap settings from zero
    collection = "heatmap"
    docs = ["specs", "gravel_index", "mtb_index", "unknown_index"]
    docs_json = [
        {
            "gravel": {
                "main_blob_name": "heatmap/gravel_v00.gpx",
                "compose_count": 0,
                "version": 0,
            },
            "mtb": {
                "main_blob_name": "heatmap/mtb_v00.gpx",
                "compose_count": 0,
                "version": 0,
            },
            "unknown": {
                "main_blob_name": "heatmap/unknown_v00.gpx",
                "compose_count": 0,
                "version": 0,
            },
        },
        {},
        {},
        {}
    ]

    fm = FirestoreMagic("firestore", collection, docs)
    for doc_name, doc_json in zip(docs, docs_json):
        fm.create_firejson(doc_json)

