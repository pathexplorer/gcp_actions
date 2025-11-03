from google.cloud import secretmanager
from google.api_core.exceptions import AlreadyExists
from functools import lru_cache
import os

@lru_cache(maxsize=1)
def get_project_id():
    return os.getenv("GCP_PROJECT_ID")


secret_client = secretmanager.SecretManagerServiceClient()

def get_secret(secret_id: str, version_id="latest"):
    """
    :param secret_id:
    :param version_id:
    :return:
    """

    name = f"projects/{get_project_id()}/secrets/{secret_id}/versions/{version_id}"
    response = secret_client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8").strip()

def create_secret(secret_id: str):
    short_parent = f"projects/{get_project_id()}"
    try:
        secret_client.create_secret(
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

def update_secret(secret_id: str, new_value):
    parent = f"projects/{get_project_id()}/secrets/{secret_id}"
    secret_client.add_secret_version(
        request={
            "parent": parent,
            "payload": {"data": new_value.encode("UTF-8")}
        }
    )