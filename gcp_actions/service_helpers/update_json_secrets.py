"""
LOCAL RUNNING CONFIGURATION
Manual script for first setup cloud secrets
"""
from gcp_actions.secret_manager import SecretManagerClient
import subprocess
import logging
from pathlib import Path
from dotenv import load_dotenv
import os

# Loading environments
token_api_env_path = "BigBikeData/dropbox_bucket_strava/project_env/token_api.env"
keys_env_path = "BigBikeData/dropbox_bucket_strava/project_env/keys.env"

current_file_dir = Path(__file__).resolve().parent
PROJECTS_ROOT = current_file_dir.parents[2]

dotenv_path = PROJECTS_ROOT / token_api_env_path
if dotenv_path.is_file():
    load_dotenv(dotenv_path=dotenv_path)
    print(f"Successfully loaded environment from: {dotenv_path}")
    # ... your code continues ...
else:
    print(f"🯀 ERROR: Environment file not found at: {dotenv_path}")
    # Exit or handle error

dotenv_path1 = PROJECTS_ROOT / keys_env_path
if dotenv_path1.is_file():
    load_dotenv(dotenv_path=dotenv_path1)
    print(f"Successfully loaded environment from: {dotenv_path1}")
    # ... your code continues ...
else:
    print(f"🯀 ERROR: Environment file not found at: {dotenv_path1}")
    # Exit or handle error

# Setup

SECRET_CONFIG_MAP = {
    "SEC_DROPBOX": [
        "DROPBOX_APP_KEY",
        "DROPBOX_APP_SECRET",
        "DROPBOX_REFRESH_TOKEN"
    ],
    "SEC_STRAVA": [
        "STRAVA_APP_ID",
        "STRAVA_CLIENT_SECRET",
        "STRAVA_REFRESH_TOKEN",
        "STRAVA_ACCESS_TOKEN",
        "EXPIRES_AT"
    ]
}



def get_gcloud_config(key: str) -> str:
    """
    LOCAL RUNNING CONFIGURATION
    Get PROJECT_ID from gcloud config
    """
    result = subprocess.run(
        ["gcloud", "config", "get-value", key],
        capture_output=True, text=True
    )
    value = result.stdout.strip()
    if not value:
        logging.warning(f"🯀 gcloud config key '{key}' returned empty.")
        raise ValueError(f"Missing gcloud config value for key: '{key}'")
    return value

# # Usage
# try:
#     project_id = get_gcloud_config("project")
#     print(f"✅ Active project: {project_id}")
# except ValueError as e:
#     print(f"❌ Error: {e}")


sm = SecretManagerClient(get_gcloud_config("project"))
for gsm_secret_env_name, payload_env_keys in SECRET_CONFIG_MAP.items():
    try:
        # 1. Get the actual Google Secret Manager secret name
        gsm_secret_name = os.environ[gsm_secret_env_name]
        print(f"\n  Processing secret: {gsm_secret_name} (from env var {gsm_secret_env_name})")

        # 2. Build the JSON payload for this specific secret
        # We use the payload_env_key as the key in the JSON object
        json_payload = {
            key: os.environ[key] for key in payload_env_keys
        }

        # 3. Update the secret with the new JSON payload
        sm.update_secret_json(gsm_secret_name, json_payload)  # This is your custom method

        print(f"    ✓ Successfully updated secret: {gsm_secret_name}")
        print(f"    - Payload Keys: {list(json_payload.keys())}")

    except KeyError as e:
        # Handle errors for missing environment variables
        print(f"  🯀 ERROR: Missing environment variable {e}. Skipping {gsm_secret_env_name}.")
    except Exception as e:
        # Handle other errors (e.g., permissions)
        print(f"  🯀 ERROR: Failed to update. Details: {e}")

print("\n▷ Secret update process finished.")



#--- 6. Read the JSON secret in your application ---
# try:
#     print(f"Fetching secret: {main_secret_id}")
#
#     # This is the main method you'll use
#     secrets_dict = sm.get_secret_json(main_secret_id)
#
#     for secret_id in secrets_dict:
#         print(f"Secret: ...{secrets_dict[secret_id][-4:]}")
# except Exception as e:
#     print(f"Error accessing secret: {e}")