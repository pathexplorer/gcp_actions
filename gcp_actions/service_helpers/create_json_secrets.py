"""
LOCAL RUNNING CONFIGURATION
Manual script for first setup cloud secrets
"""
from gcp_actions.secret_manager import SecretManagerClient
import subprocess
import logging

def get_gcloud_config(key: str) -> str:
    """
    LOCAL RUNNING CONFIGURATION
    Get PROJECT_ID from gcloud config
    :param key:
    :return:
    """
    result = subprocess.run(
        ["gcloud", "config", "get-value", key],
        capture_output=True, text=True
    )
    value = result.stdout.strip()
    if not value:
        logging.warning(f"⚠️ gcloud config key '{key}' returned empty.")
        raise ValueError(f"Missing gcloud config value for key: '{key}'")
    return value

# # Usage
# try:
#     project_id = get_gcloud_config("project")
#     print(f"✅ Active project: {project_id}")
# except ValueError as e:
#     print(f"❌ Error: {e}")


sm = SecretManagerClient(get_gcloud_config("project"))
main_secret_id = "telegram-secrets"

name_variables = [
    'API_ID',
    'API_HASH',
    'BOT_TOKEN',
    'session_string'
]
content_of_secrets = [
    '..',
    '..',
    '..',
    '..'
]


all_telegram_secrets = {key: value for key, value in zip(name_variables, content_of_secrets)}
print(all_telegram_secrets)




# Create the secret container
#sm.create_secret(main_secret_id)

# 5. Update the secret with your new JSON data
#sm.update_secret_json(main_secret_id, all_telegram_secrets)


#--- 6. Read the JSON secret in your application ---
try:
    print(f"Fetching secret: {main_secret_id}")

    # This is the main method you'll use
    secrets_dict = sm.get_secret_json(main_secret_id)

    for secret_id in secrets_dict:
        print(f"Secret: ...{secrets_dict[secret_id][-4:]}")
except Exception as e:
    print(f"Error accessing secret: {e}")