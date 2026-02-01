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
    """
    Robustly finds the project root by searching upwards for a marker file/dir.
    Here, we use '.git' as the marker for the project root.
    """
    try:
        current_dir = Path(sys.argv[0]).resolve().parent
    except (IndexError, AttributeError):
        current_dir = Path.cwd()

    while current_dir != current_dir.parent:
        if (current_dir / '.git').is_dir():
            return current_dir
        current_dir = current_dir.parent

    logger.warning("Could not find project root (.git directory). Local overrides may not be found.")
    return {}

class InjectConfig:
    def __init__(self,
            list_of_secret_env_vars: list = None,
            list_of_sa_env_vars: list = None,
            from_firestore: bool = True
    ) -> dict[Any, Any] | None:

        self.list_of_secret_env_vars = list_of_secret_env_vars
        self.list_of_sa_env_vars = list_of_sa_env_vars
        self.from_firestore = from_firestore

        if self.list_of_sa_env_vars is None:
            self.list_of_sa_env_vars = []

        self.project_id = os.getenv("GCP_PROJECT_ID")
        if not self.project_id:
            raise EnvironmentError("GCP_PROJECT_ID not set in environment")
        logger.debug(f"Starting initial configuration load for project: {self.project_id}")

    def _inject_firestore(self):
        """
        Load base config from the Firestore
        :return: firestore_config
        """
        firestore_config = {}
        if self.from_firestore:
            fs = FirestoreMagic("config", "local/settings/data")
            firestore_config = fs.load_firejson()
            logger.info(f"📦 Loaded from Firestore: {len(firestore_config)} keys.")
            return firestore_config
        else:
            logger.debug("Skipping Firestore config load.")
            return {}


    def add_local_variables(self, merged_config = None):
        project_root = _find_project_root()
        if project_root:
            if not merged_config:
                merged_config = {}
            local_override_path = project_root / 'local_config.json'
            if local_override_path.is_file():
                try:
                    with open(local_override_path, 'r') as f:
                        local_overrides = json.load(f)
                    merged_config.update(local_overrides)
                    logger.warning(f"✅ Applied {len(local_overrides)} overrides from '{local_override_path}'.")
                except (json.JSONDecodeError, IOError) as e:
                    logger.error(f"❌ Failed to load local overrides from '{local_override_path}': {e}")
            else:
                logger.info("No local override file found. Using production/default config.")

    def _final_merge(self, merged_config):
        # 5. Inject the final merged config into the environment
        for key, value in merged_config.items():
            os.environ[key] = str(value)

        logger.info(f"✅ Injected a total of {len(merged_config)} configuration values into environment.")

        return merged_config

    @run_timer
    def load_and_inject_config(self):

        """
        Loads configuration from Firestore and Secret Manager, then applies local overrides.
        The order of precedence is: Local Overrides > Secrets > Firestore.
        """
        # Start the first part
        firestore_config = self._inject_firestore()

        # 2. Load secrets from Secret Manager
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

                    logger.debug(f"Processing secret...")
                    sm = SecretManagerClient(self.project_id, service_account_email)
                    current_secret_data = sm.get_secret_json(secret_name)

                    if current_secret_data:
                        all_secrets_data.update(current_secret_data)
                        logger.info(f"📦 Loaded from Secret Manager: {len(current_secret_data)} keys.")

            except Exception as e:
                logger.error(f"❌ Failed during secret loading: {e}", exc_info=True)
        else:
            logger.debug("No secrets specified to load.")

        # 3. Merge configurations (Secrets overwrite Firestore)
        merged_config = {**firestore_config, **all_secrets_data}

        # 4. Apply local overrides from the project root
        # Possibility run only these two last parts for only loads some additional locale variables
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
