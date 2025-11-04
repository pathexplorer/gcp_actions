from google.cloud import secretmanager
from google.api_core.exceptions import AlreadyExists

class SecretManagerClient:
    # 1. Inject the ID into the class constructor
    def __init__(self, project_id: str):
        self.project_id = project_id
        self.secret_client = secretmanager.SecretManagerServiceClient()

    def get_secret(self, secret_id: str, version_id="latest", dcoding: str = 'yes'):
        """Get secret from GCP API
        :param secret_id: "your-secret-id"
        :type secret_id: str
        :param version_id: GCP version ID
        :type version_id: str
        :param dcoding: yes (is string UTF-8) or no (raw bytes)
        :type dcoding: str
        """
        name = f"projects/{self.project_id}/secrets/{secret_id}/versions/{version_id}"
        response = self.secret_client.access_secret_version(request={"name": name})
        data = response.payload.data
        if dcoding == "yes":
            try:
                return data.decode("utf-8").strip()
            except UnicodeDecodeError as e:
                raise ValueError(f"Secret '{secret_id}' is not valid UTF-8: {e}")
        elif dcoding == "no":
            return data  # Return raw bytes
        else:
            raise ValueError(f"Invalid 'ncoding' value: {dcoding}")

    def create_secret(self, secret_id: str):
        short_parent = f"projects/{self.project_id}"
        try:
            self.secret_client.create_secret(
                request={
                    "parent": short_parent,
                    "secret_id": secret_id,
                    "secret": {
                        "replication": {"automatic": {}}
                    },
                }
            )
        except AlreadyExists:
            print(f"Secret '{secret_id}' already exists. Skipping creation.")

    def update_secret(self, secret_id: str, new_value):
        parent = f"projects/{self.project_id}/secrets/{secret_id}"
        self.secret_client.add_secret_version(
            request={
                "parent": parent,
                "payload": {"data": new_value.encode("UTF-8")}
            }
        )