"""
Configuration loader merging Firestore, Secret Manager, and local overrides.

Precedence (highest to lowest): local_config.json > Secret Manager > Firestore.
Loads secret names from local config first, then fetches actual secret values.
"""

import os
import itertools
import json
import sys
from pathlib import Path
from gcp_actions.firestore_box.json_manipulations import FirestoreMagic
from gcp_actions.secret_manager import SecretManagerClient
from typing import Any
from gcp_actions.common_utils.timer import run_timer

import logging
logger = logging.getLogger(__name__)


@run_timer
def _find_project_root() -> Path | None:
    """Find project root by searching upward for .git directory."""
    try:
        current_dir = Path(sys.argv[0]).resolve().parent
    except (IndexError, AttributeError):
        current_dir = Path.cwd()

    while current_dir != current_dir.parent:
        if (current_dir / '.git').is_dir():
            return current_dir
        current_dir = current_dir.parent

    logger.warning("Could not find project root (.git directory). Local overrides may not be found.")
    return None

def _load_local_config() -> dict:
    """Load local_config.json from project root if it exists."""
    project_root = _find_project_root()
    if not project_root:
        return {}
    local_override_path = project_root / 'local_config.json'
    if not local_override_path.is_file():
        return {}
    try:
        with open(local_override_path, 'r') as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError) as e:
        logger.error(f"❌ Failed to load local overrides from '{local_override_path}': {e}")
        return {}

class InjectConfig:
    """Load and merge config from Firestore, Secret Manager, and local overrides."""

    def __init__(
            self,
            list_of_secret_env_vars: list = None,
            list_of_sa_env_vars: list = None,
            from_firestore: bool = True
    ) -> None:
        """Initialize config loader with secret sources and project ID resolution.

        Args:
            list_of_secret_env_vars: Env var names holding Secret Manager secret IDs.
            list_of_sa_env_vars: Corresponding service accounts for secret access.
            from_firestore: Whether to load base config from Firestore.
        """
        self.list_of_secret_env_vars = list_of_secret_env_vars
        self.list_of_sa_env_vars = list_of_sa_env_vars
        self.from_firestore = from_firestore

        if self.list_of_sa_env_vars is None:
            self.list_of_sa_env_vars = []

        # Load GCP_PROJECT_ID from env, falling back to local_config.json (local dev).
        # In production this is a Cloud Run env var; locally it lives in local_config.json.
        self.project_id = os.getenv("GCP_PROJECT_ID")
        if not self.project_id:
            local_config = _load_local_config()
            self.project_id = local_config.get("GCP_PROJECT_ID")
            if self.project_id:
                os.environ["GCP_PROJECT_ID"] = self.project_id
                logger.info(f"GCP_PROJECT_ID loaded from local_config.json: {self.project_id}")

        if not self.project_id:
            raise EnvironmentError(
                "GCP_PROJECT_ID not set. In production set it as a Cloud Run env var; "
                "locally add it to local_config.json."
            )
        logger.debug(f"Starting initial configuration load for project: {self.project_id}")

    def _inject_firestore(self) -> dict:
        """Load base configuration from Firestore config/local/settings/data."""
        firestore_config = {}
        if self.from_firestore:
            fs = FirestoreMagic("config", "local/settings/data")
            firestore_config = fs.load_firejson()
            logger.info(f"📦 Loaded from Firestore: {len(firestore_config)} keys.")
            return firestore_config
        else:
            logger.debug("Skipping Firestore config load.")
            return {}


    def add_local_variables(self, merged_config: dict | None = None) -> dict:
        """Merge local_config.json overrides into the config dict."""
        if merged_config is None:
            merged_config = {}
        local_overrides = _load_local_config()
        if local_overrides:
            merged_config.update(local_overrides)
            logger.warning(f"✅ Applied {len(local_overrides)} overrides from 'local_config.json'.")
        else:
            logger.info("No local override file found. Using production/default config.")
        return merged_config

    def _final_merge(self, merged_config: dict) -> dict:
        """Inject merged configuration into environment variables."""
        for key, value in merged_config.items():
            os.environ[key] = str(value)

        logger.info(f"✅ Injected a total of {len(merged_config)} configuration values into environment.")

        return merged_config

    @run_timer
    def load_and_inject_config(self) -> dict:
        """Load and merge config: Firestore -> local -> secrets -> local (final).

        Precedence: local_config.json > Secret Manager > Firestore.
        Local config applied twice: first for secret names, then for final overrides.
        """
        # 1. Load base config from Firestore
        firestore_config = self._inject_firestore()

        # 2. Apply local overrides FIRST — this populates secret *name* env vars
        #    (e.g. APP_JSON_KEYS=fullstack-app-json-keys) so we know which secrets to fetch.
        merged_config = {**firestore_config}
        self.add_local_variables(merged_config)
        self._final_merge(merged_config)

        # 3. NOW load secrets from Secret Manager (secret names are available)
        all_secrets_data = {}
        if self.list_of_secret_env_vars:
            try:
                for secret_env_var, sa_env_var in itertools.zip_longest(
                        self.list_of_secret_env_vars,
                        self.list_of_sa_env_vars,
                        fillvalue=None
                ):
                    secret_name = os.getenv(secret_env_var)
                    service_account_email = os.getenv(sa_env_var) if sa_env_var else None

                    if not secret_name:
                        logger.warning(f"Env var '{secret_env_var}' for secret name is not set. Skipping.")
                        continue

                    logger.debug(f"Processing secret '{secret_name}' ({secret_env_var})...")
                    sm = SecretManagerClient(self.project_id, service_account_email)
                    current_secret_data = sm.get_secret_json(secret_name)

                    if current_secret_data:
                        all_secrets_data.update(current_secret_data)
                        logger.info(f"📦 Loaded from Secret Manager: {len(current_secret_data)} keys.")

            except Exception as e:
                logger.error(f"❌ Failed during secret loading: {e}", exc_info=True)
        else:
            logger.debug("No secrets specified to load.")

        # 4. Merge secrets on top (overwriting Firestore/local defaults with real secrets)
        merged_config.update(all_secrets_data)

        # 5. Re-apply local overrides on top of secrets (local wins over everything)
        self.add_local_variables(merged_config)
        self._final_merge(merged_config)


# if __name__ == "__main__":
#     from gcp_actions.common_utils.local_runner import check_cloud_or_local_run
#     check_cloud_or_local_run()
#
#     print("\n" + "=" * 60)
#     print("Testing Config Loading")
#     print("=" * 60 + "\n")
#     list_of_secret_env_vars1 = ["APP_JSON_KEYS", "SEC_DROPBOX"]
#     list_of_sa_env_vars1 = [None, "S_ACCOUNT_DROPBOX"]
#     config = load_and_inject_config(list_of_secret_env_vars1, list_of_sa_env_vars1)
#
#     print("\n📋 Final Environment Configuration:")
#     # Test a few key variables
#     print(f"  DROPBOX_TOPIC_NAME: {os.environ.get('DROPBOX_TOPIC_NAME')}")
#     print(f"  LOGGING_LEVEL: {os.environ.get('LOGGING_LEVEL')}")
#     print(f"  GCS_BUCKET_NAME: {os.environ.get('GCS_BUCKET_NAME')}")
