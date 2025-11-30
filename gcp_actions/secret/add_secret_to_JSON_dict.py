import os

import logging
from gcp_actions.secret_manager import SecretManagerClient
from gcp_actions.common_utils.init_config import load_and_inject_config

list_of_secret_env_vars = ["APP_JSON_KEYS"]
load_and_inject_config(list_of_secret_env_vars)
logger = logging.getLogger(__name__)


def add_or_update_secret_key(project_id: str, secret_id: str, key: str, value: str) -> bool:
    """
    Adds or updates a specific key-value pair in a JSON-based Google Secret Manager secret.

    This function fetches the secret, decodes the JSON, updates the key,
    and writes the new version back.

    Args:
        project_id: The Google Cloud project ID.
        secret_id: The ID of the secret to update.
        key: The key within the JSON dictionary to add or update.
        value: The new value to associate with the key.

    Returns:
        True if the secret was updated successfully, False otherwise.
    """
    if not all([project_id, secret_id, key]):
        logger.error("Project ID, Secret ID, and Key are required.")
        return False

    try:
        # Initialize the client
        sm_client = SecretManagerClient(project_id)
        logger.info(f"Attempting to update key '{key}' in secret '{secret_id}' in project '{project_id}'.")

        # Get the current secret dictionary
        secret_dict = sm_client.get_secret_json(secret_id)

        # Add or update the key
        secret_dict[key] = value

        # Update the secret with the modified dictionary
        sm_client.update_secret_json(secret_id, secret_dict)

        logger.info(f"Successfully updated key '{key}' in secret '{secret_id}'.")
        return True

    except Exception as e:
        logger.critical(
            f"Failed to update secret '{secret_id}' in project '{project_id}'. Error: {e}",
            exc_info=True  # Include stack trace for better debugging
        )
        return False


if __name__ == '__main__':
    # Example Usage: Replace with your actual project details and secret info
    # Make sure you have authenticated with `gcloud auth application-default login`

    GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
    SECRET_ID = os.getenv("APP_JSON_KEYS")
    NEW_KEY = ""
    NEW_VALUE = ""



    success = add_or_update_secret_key(GCP_PROJECT_ID, SECRET_ID, NEW_KEY, NEW_VALUE)

    if success:
        print("Secret update process completed successfully.")
    else:
        print("Secret update process failed. Check logs for details.")
    pass
