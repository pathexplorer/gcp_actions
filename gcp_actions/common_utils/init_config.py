import os
import itertools
from gcp_actions.common_utils.local_runner import check_cloud_or_local_run
from gcp_actions.firestore_as_swith import load_config_from_firestore
from gcp_actions.secret_manager import SecretManagerClient

import logging
logger = logging.getLogger(__name__)

check_cloud_or_local_run()


def load_and_inject_config(list_of_secret_env_vars: list, list_of_sa_env_vars: list = None):
    """
    Process a list of name variables (from YAML) and load it to env. Use only at entrance point (main.py)
    Os.environ already presents the
    :param list_of_secret_env_vars: name of variables, which are loaded in YAML
    :param list_of_sa_env_vars: name of service accounts if it is used separately
    :return:
    """
    if list_of_sa_env_vars is None:
        list_of_sa_env_vars = []

    project_id = os.getenv("GCP_PROJECT_ID")
    if not project_id:
        raise EnvironmentError("GCP_PROJECT_ID not set in environment")
    logger.debug("Loading configuration from Firestore and Secret Manager...")

    firestore_config = load_config_from_firestore()
    logger.info(f"📦 Loaded from Firestore: {len(firestore_config)}")

    all_secrets_data = {}
    try:
        for secret_env_var, sa_env_var in itertools.zip_longest(
                list_of_secret_env_vars,
                list_of_sa_env_vars,
                fillvalue=None
        ):
            secret_name = os.getenv(secret_env_var)
            service_account_email = os.getenv(sa_env_var) if sa_env_var else None

            if not secret_name:
                logger.warning(f"Env var '{secret_env_var}' for secret name is not set. Skipping.")
                continue

            logger.debug(f"Processing secret...")
            sm = SecretManagerClient(project_id, service_account_email)
            current_secret_data = sm.get_secret_json(secret_name)

            # Use .update() to merge the new data, not overwrite it
            if current_secret_data:
                all_secrets_data.update(current_secret_data)
                logger.info(f"📦 Loaded from Secret Manager: {len(current_secret_data)} keys.")

    except Exception as e:
        logger.error(f"❌ Failed during secret loading: {e}", exc_info=True)
        pass

        # Merge Firestore config with the aggregated secret data
    merged_config = {**firestore_config, **all_secrets_data}

    if merged_config:
        for key, value in merged_config.items():
            os.environ[key] = str(value)
        logger.info(f"✅ Injected a total of {len(merged_config)} configuration values into environment.")

    return merged_config

# GCS_BUCKET_NAME = None
# EMAIL_MODE = None
# GCS_PUB_INPUT_BUCKET = None
# GCS_PUB_OUTPUT_BUCKET = None
# GCP_TOPIC_NAME = None
#
# def init_config_vars():
#     """
#     Convenience function to populate module-level variables
#     after configuration has been injected into os.environ.
#     """
#     global GCS_BUCKET_NAME, EMAIL_MODE, GCS_PUB_INPUT_BUCKET
#     global GCS_PUB_OUTPUT_BUCKET, GCP_TOPIC_NAME
#
#     GCS_BUCKET_NAME = os.environ.get("CS_BUCKET_NAME")
#     EMAIL_MODE = os.environ.get("EMAIL_MODE")
#     GCS_PUB_INPUT_BUCKET = os.environ.get("GCS_PUB_INPUT_BUCKET")
#     GCS_PUB_OUTPUT_BUCKET = os.environ.get("GCS_PUB_OUTPUT_BUCKET")
#     GCP_TOPIC_NAME = os.environ.get("GCP_TOPIC_NAME")
#
#     logger.info("Module-level config variables initialized")
#

if __name__ == "__main__":
    # Test the config loading
    from gcp_actions.common_utils.local_runner import check_cloud_or_local_run

    check_cloud_or_local_run()

    print("\n" + "=" * 60)
    print("Testing Config Loading")
    print("=" * 60 + "\n")
    list_of_secret_env_vars1 = ["APP_JSON_KEYS", "SEC_DROPBOX"]
    list_of_sa_env_vars1 = [None, "S_ACCOUNT_DROPBOX"]
    config = load_and_inject_config(list_of_secret_env_vars1, list_of_sa_env_vars1)

    print("\n📋 Loaded Configuration:")
    for key1, value1 in config.items():
        # Mask sensitive values
        if any(secret in key1.lower() for secret in ['password', 'secret', 'key', 'token', 'api']):
            print(f"  {key1}: ***REDACTED***")
        else:
            print(f"  {key1}: {value1}")

    print("\n🧪 Testing os.environ access:")
    print(f"  GCS_PUB_INPUT_BUCKET: {os.environ.get('GCS_PUB_INPUT_BUCKET')}")
    print(f"  EMAIL_MODE: {os.environ.get('EMAIL_MODE')}")
    print(f"  GCS_BUCKET_NAME: {os.environ.get('GCS_BUCKET_NAME')}")