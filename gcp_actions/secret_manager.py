from google.cloud import secretmanager
from google.api_core.exceptions import AlreadyExists
import json
from json.decoder import JSONDecodeError

class SecretManagerClient:
    """
    Client for managing and accessing GCP Secrets, with helpers for
    handling secrets stored as plain text which code in UTF-8 (standard usage),
    plain text without any code, and as JSON strings.
    Usage:
        1. Import:
        from gcp_actions.secret_manager import SecretManagerClient
        from gcp_actions.client import get_env_and_cashed_it
        2. Access:
        sm = SecretManagerClient(get_env_and_cashed_it("GCP_PROJECT_ID"))
        access_dict = sm.get_secret_json("{name of secret}")
    """

    # Inject the Project_ID into the class constructor
    def __init__(self, project_id: str):
        self.project_id = project_id
        self.secret_client = secretmanager.SecretManagerServiceClient()

    def get_secret_json(self, secret_id: str) -> dict:
        """
        Gets the latest secret, decodes it, and parses it as JSON.

        :return: A Python dictionary.
        """
        try:
            # First, get the raw string value
            secret_string = self.get_secret_string(secret_id)
            # Then, parse the string as JSON
            return json.loads(secret_string)
        except JSONDecodeError as e:
            # Raise a specific error if the content isn't valid JSON
            raise ValueError(f"Secret '{secret_id}' payload is not valid JSON: {e}")

    def update_secret_json(self, secret_id: str, new_data_dict: dict):
        """
        Adds a new secret version from a Python dictionary.
        The dictionary will be converted to a JSON string.

        :param secret_id: The ID of the secret to update.
        :param new_data_dict: The Python dictionary to store.
        """
        # Convert the dictionary to a JSON string
        # 'indent=2' makes it human-readable in the GCP console
        json_string = json.dumps(new_data_dict, indent=2)

        # Call the string update method
        self.update_secret_string(secret_id, json_string)
        print(f"Secret '{secret_id}' updated with new JSON version.")


    def get_secret_string(self, secret_id: str, version_id="latest", utf_coding: str = 'yes'):
        """Get secret from GCP API
        :param secret_id: "your-secret-id"
        :param version_id: GCP version ID
        :param utf_coding: yes (is string UTF-8) or no (raw bytes, as sample session file)
        """
        name = f"projects/{self.project_id}/secrets/{secret_id}/versions/{version_id}"
        response = self.secret_client.access_secret_version(request={"name": name})
        data = response.payload.data
        if utf_coding == "yes":
            try:
                return data.decode("utf-8").strip()
            except UnicodeDecodeError as e:
                raise ValueError(f"Secret '{secret_id}' is not valid UTF-8: {e}")
        elif utf_coding == "no":
            return data  # Return raw bytes
        else:
            raise ValueError(f"Invalid 'utf_coding' value: {utf_coding}")



    def update_secret_string(self, secret_id: str, new_value):
        parent = f"projects/{self.project_id}/secrets/{secret_id}"
        self.secret_client.add_secret_version(
            request={
                "parent": parent,
                "payload": {"data": new_value.encode("UTF-8")}
            }
        )

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