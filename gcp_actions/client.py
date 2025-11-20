from google.cloud import storage
from google.auth import impersonated_credentials
from google.auth import default as default_credentials
from functools import lru_cache
import os

from gcp_actions import local_runner as lr

lr.check_cloud_or_local_run()

@lru_cache(maxsize=8)
def get_env_and_cashed_it(variable: str):
    """
    Loads and caches variable from the environment
    :param variable: "GCP_PROJECT_ID" or gcp_project_id (which == "GCP_PROJECT_ID")
    """
    get_variable = os.getenv(variable)
    if not get_variable:
        raise EnvironmentError(f"{variable} environment variable not set. Please configure your environment.")
    return get_variable


@lru_cache(maxsize=8)
def get_client(target_principal: str | None = None):
    """
    Creates a GCS client. If a target_principal (service account email) is provided,
    the client will be configured to impersonate that service account.
    This is necessary for signing URLs.
    """
    # Get the default credentials of the environment (e.g., the Cloud Run service's identity)
    creds, project = default_credentials()

    if target_principal:
        print(f"DEBUG: Creating client with impersonation for: {target_principal}")
        # Create new credentials by impersonating the target service account
        creds = impersonated_credentials.Credentials(
            source_credentials=creds,
            target_principal=target_principal,
            target_scopes=["https://www.googleapis.com/auth/devstorage.full_control"],
        )

    return storage.Client(credentials=creds)


@lru_cache(maxsize=8)
def get_bucket(bucket_name: str, user_project: str | None = None, impersonate_sa: str | None = None):
    """
    Gets a bucket handle, supporting Requester Pays and impersonation.
    """
    # Try to get the bucket name from an environment variable.
    actual_bucket_name = os.getenv(bucket_name)

    # If the environment variable wasn't found, assume the provided name is the actual name.
    if not actual_bucket_name:
        actual_bucket_name = bucket_name

    if not actual_bucket_name:
        raise ValueError("The bucket name cannot be empty.")

    # Get a client, potentially with impersonation
    client = get_client(target_principal=impersonate_sa)
    
    return client.bucket(actual_bucket_name, user_project=user_project)
