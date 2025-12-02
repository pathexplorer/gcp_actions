from google.cloud import secretmanager
from google.api_core.exceptions import AlreadyExists
import json
from google.auth import impersonated_credentials
from google.auth.transport.requests import AuthorizedSession
from google.auth import default  # To get the runtime credentials
import logging

logger = logging.getLogger(__name__)

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
        If you use one service account:
        sm = SecretManagerClient(get_env_and_cashed_it("GCP_PROJECT_ID"))
        If you use many service accounts for certain serviced
        sm = SecretManagerClient(
            get_env_and_cashed_it("GCP_PROJECT_ID"),
            full email specify service account)
        access_dict = sm.get_secret_json("{name of secret}")
    """

    # Inject the Project_ID into the class constructor
    def __init__(self, project_id: str, target_sa_email: str = None):
        """
        Initializes the client.

        :param project_id: The GCP project ID.
        :param target_sa_email: The email of the Service Account to impersonate.
                                If None, the client uses default credentials.
        """
        self.project_id = project_id

        if target_sa_email:
            logger.info(f"Initializing client with impersonation: {target_sa_email[:7]}")
            credentials, transport = self._create_impersonated_credentials(target_sa_email)
            self.secret_client = secretmanager.SecretManagerServiceClient(
                credentials=credentials
            )
        else:
            logger.info("Initializing client with default credentials (Cloud Run Runtime SA).")
            # If target_sa_email is None, use the default credentials (e.g., Cloud Run SA)
            self.secret_client = secretmanager.SecretManagerServiceClient()

    @staticmethod
    def _create_impersonated_credentials(target_sa_email: str):
        """
        Generates short-lived credentials for the target Service Account.
        """
        # The Runtime SA needs the 'roles/iam.serviceAccountUser' role on the target SA

        # 1. Get the source credentials (the identity the code is running as)
        source_creds, _ = default(
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        # 2. Create the impersonated credentials object
        impersonated_creds = impersonated_credentials.Credentials(
            source_credentials=source_creds,
            target_principal=target_sa_email,
            target_scopes=["https://www.googleapis.com/auth/cloud-platform"],
            lifetime=300  # 5 minutes is generally sufficient for a single call
        )
        # 3. Create an AuthorizedSession transport layer
        http_session = AuthorizedSession(impersonated_creds)

        return impersonated_creds, http_session

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
        except json.JSONDecodeError as e:
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
        logger.info(f"Secret updated with new JSON version.")

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
            logger.warning(f"Secret already exists. Skipping creation.")