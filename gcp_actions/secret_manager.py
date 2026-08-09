"""
Secret Manager client with support for GCP production and local emulator.

Provides typed access to secrets stored as JSON, plain text, or raw bytes.
Supports service account impersonation for cross-project secret access.
"""

from google.cloud import secretmanager
from google.api_core.exceptions import AlreadyExists
import json
import os
import urllib.request
import urllib.error
from google.auth import impersonated_credentials
from google.auth.transport.requests import AuthorizedSession
from google.auth import default  # To get the runtime credentials
import logging
from gcp_actions.common_utils.timer import run_timer

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Emulator support
# ---------------------------------------------------------------------------
# When SECRET_MANAGER_EMULATOR_HOST is set, the client will use a simple
# HTTP-based emulator instead of the real GCP Secret Manager gRPC API.
# Expected format: "localhost:8083" or "http://localhost:8083"
# ---------------------------------------------------------------------------


class SecretManagerClient:
    """Client for managing and accessing GCP Secret Manager secrets.

    Supports three payload formats:
    - JSON (get_secret_json / update_secret_json)
    - UTF-8 text (get_secret_string with utf_coding="yes")
    - Raw bytes (get_secret_string with utf_coding="no")

    Emulator mode is activated by setting SECRET_MANAGER_EMULATOR_HOST
    environment variable (e.g., "localhost:8083").
    """

    # Inject the Project_ID into the class constructor
    @run_timer
    def __init__(self, project_id: str, target_sa_email: str | None = None):
        """Initialize the Secret Manager client.

        Args:
            project_id: GCP project ID containing the secrets.
            target_sa_email: Optional service account to impersonate for access.
        """
        self.project_id = project_id

        # --- Emulator mode detection ---
        emulator_host = os.environ.get("SECRET_MANAGER_EMULATOR_HOST", "").strip()
        self._use_emulator = bool(emulator_host)
        if self._use_emulator:
            # Normalise: ensure it has a scheme
            if not emulator_host.startswith("http"):
                emulator_host = f"http://{emulator_host}"
            self._emulator_base = emulator_host.rstrip("/")
            logger.info(
                "🔧 Secret Manager EMULATOR mode — using %s (project=%s)",
                self._emulator_base,
                self.project_id,
            )
            self.secret_client = None  # no gRPC client needed
            return

        # --- Real GCP mode ---
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
        """Create short-lived impersonated credentials for a target service account."""
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
        """Fetch the latest secret version and parse it as JSON."""
        try:
            # First, get the raw string value
            secret_string = self.get_secret_string(secret_id)
            # Then, parse the string as JSON
            return json.loads(secret_string)
        except json.JSONDecodeError as e:
            # Raise a specific error if the content isn't valid JSON
            raise ValueError(f"Secret '{secret_id}' payload is not valid JSON: {e}")

    def update_secret_json(self, secret_id: str, new_data_dict: dict):
        """Add a new secret version from a Python dictionary (serialized as JSON)."""
        # Convert the dictionary to a JSON string
        # 'indent=2' makes it human-readable in the GCP console
        json_string = json.dumps(new_data_dict, indent=2)

        # Call the string update method
        self.update_secret_string(secret_id, json_string)
        logger.info(f"Secret updated with new JSON version.")
    @run_timer
    def get_secret_string(self, secret_id: str, version_id: str = "latest", utf_coding: str = "yes") -> str | bytes:
        """Fetch a secret version as UTF-8 string or raw bytes.

        Args:
            secret_id: Secret name in Secret Manager.
            version_id: Version to fetch (default: "latest").
            utf_coding: "yes" to decode as UTF-8 string, "no" for raw bytes.

        Returns:
            Decoded string or raw bytes depending on utf_coding.

        Raises:
            ValueError: If utf_coding is not "yes" or "no", or decoding fails.
        """
        if self._use_emulator:
            return self._emulator_access_secret(secret_id, version_id, utf_coding)

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

    def update_secret_string(self, secret_id: str, new_value: str):
        """Add a new version to an existing secret with the given string value."""
        if self._use_emulator:
            self._emulator_add_version(secret_id, new_value)
            return

        parent = f"projects/{self.project_id}/secrets/{secret_id}"
        self.secret_client.add_secret_version(
            request={
                "parent": parent,
                "payload": {"data": new_value.encode("UTF-8")}
            }
        )

    def create_secret(self, secret_id: str):
        """Create a new secret with automatic replication if it doesn't exist."""
        if self._use_emulator:
            self._emulator_create_secret(secret_id)
            return

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

    # ------------------------------------------------------------------
    # Emulator helper methods (HTTP-based)
    # ------------------------------------------------------------------
    def _emulator_request(self, method: str, path: str, body: dict | None = None) -> dict:
        """Send an HTTP request to the emulator and return parsed JSON response."""
        url = f"{self._emulator_base}{path}"
        data = None
        headers = {"Content-Type": "application/json"}
        if body is not None:
            data = json.dumps(body).encode("utf-8")

        req = urllib.request.Request(url, data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                raw = resp.read().decode("utf-8")
                return json.loads(raw) if raw else {}
        except urllib.error.HTTPError as e:
            error_body = e.read().decode("utf-8", errors="replace")
            if e.code == 409:  # AlreadyExists → ignore (like production)
                logger.warning("Emulator: secret already exists, skipping creation.")
                return {}
            if e.code == 404:
                raise ValueError(
                    f"Secret not found in emulator at path '{path}'. "
                    f"Did you seed the emulator? Body: {error_body}"
                )
            raise RuntimeError(
                f"Emulator HTTP {e.code} on {method} {url}: {error_body}"
            ) from e
        except urllib.error.URLError as e:
            raise ConnectionError(
                f"Cannot reach Secret Manager emulator at {self._emulator_base}. "
                f"Is the container running? ({e})"
            ) from e

    def _emulator_access_secret(self, secret_id: str, version_id: str, utf_coding: str) -> str:
        """Retrieve a secret version from the emulator via GET request."""
        path = f"/v1/projects/{self.project_id}/secrets/{secret_id}/versions/{version_id}"
        resp = self._emulator_request("GET", path)
        data = resp.get("payload", {}).get("data", "")
        if utf_coding == "no":
            return data.encode("utf-8") if isinstance(data, str) else data
        return data.strip() if isinstance(data, str) else str(data)

    def _emulator_add_version(self, secret_id: str, value: str):
        """Add a new version to a secret in the emulator via POST request."""
        path = f"/v1/projects/{self.project_id}/secrets/{secret_id}:addVersion"
        self._emulator_request("POST", path, body={"payload": {"data": value}})

    def _emulator_create_secret(self, secret_id: str):
        """Create a new secret in the emulator via POST request."""
        path = f"/v1/projects/{self.project_id}/secrets"
        self._emulator_request(
            "POST", path,
            body={"secret_id": secret_id, "secret": {"replication": {"automatic": {}}}},
        )
    # ------------------------------------------------------------------