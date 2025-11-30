from google.cloud import storage, firestore
from google.api_core import exceptions as google_exceptions
from google.auth import impersonated_credentials, default as default_credentials
from google.auth.exceptions import DefaultCredentialsError
from functools import lru_cache
import os
import logging

logger = logging.getLogger(__name__)

# This import is assumed to be correct based on the original file.
from gcp_actions.common_utils import local_runner as lr
lr.check_cloud_or_local_run()


@lru_cache(maxsize=8)
def get_env_and_cashed_it(variable: str) -> str:
    """
    Loads and caches a required variable from the environment.

    Args:
        variable: The name of the environment variable (e.g., "GCP_PROJECT_ID").

    Returns:
        The value of the environment variable.

    Raises:
        ValueError: If the variable name is empty or not a string.
        EnvironmentError: If the environment variable is not set.
    """
    if not variable or not isinstance(variable, str):
        raise ValueError("The environment variable name must be a non-empty string.")
    
    try:
        get_variable = os.getenv(variable)
        if get_variable is None:
            raise EnvironmentError(f"'{variable}' environment variable not set. Please configure your environment.")
        return get_variable
    except Exception as e:
        logger.error(f"An unexpected error occurred while accessing env var '{variable}': {e}")
        raise


@lru_cache(maxsize=8)
def get_any_client(client_name: str, target_principal: str | None = None):
    """
    Client Factory: Creates and caches a Google Cloud client, with optional impersonation.

    Args:
        client_name: The name of the client to create. Supported: "firestore", "storage".
        target_principal: The email of the service account to impersonate.

    Returns:
        An initialized Google Cloud client instance.

    Raises:
        ValueError: If the client_name is not supported.
        RuntimeError: If client creation fails due to credential or permission issues.
    """
    client_map = {"firestore": firestore.Client, "storage": storage.Client}
    client_name_lower = client_name.lower()

    if client_name_lower not in client_map:
        raise ValueError(f"Unknown client type '{client_name}'. Supported types are: {list(client_map.keys())}")

    try:
        creds, project = default_credentials()

        if target_principal:
            logger.debug(f"Impersonating '{target_principal}' for {client_name} client.")
            scopes = {
                "firestore": ["https://www.googleapis.com/auth/datastore", "https://www.googleapis.com/auth/cloud-platform"],
                "storage": ["https://www.googleapis.com/auth/devstorage.full_control"]
            }.get(client_name_lower, ["https://www.googleapis.com/auth/cloud-platform"])

            creds = impersonated_credentials.Credentials(
                source_credentials=creds,
                target_principal=target_principal,
                target_scopes=scopes,
            )

        client_class = client_map[client_name_lower]
        loaded_client = client_class(credentials=creds)
        logger.debug(f"Successfully initialized {client_name} client.")
        return loaded_client

    except DefaultCredentialsError:
        logger.critical("GCP authentication failed. Could not find default credentials.")
        raise RuntimeError("GCP authentication failed. Configure credentials with 'gcloud auth application-default login'.")
    except Exception as e:
        logger.error(f"Failed to initialize {client_name} client: {e}. Check IAM permissions and configuration.")
        raise RuntimeError(f"Could not create GCP client '{client_name}'.") from e


@lru_cache(maxsize=8)
def get_bucket(bucket_name: str, user_project: str | None = None, impersonate_sa: str | None = None) -> storage.Bucket:
    """
    Gets a Google Cloud Storage bucket handle, supporting Requester Pays and impersonation.

    Args:
        bucket_name: The literal bucket name or an environment variable containing the name.
        user_project: The project ID to bill for Requester Pays requests.
        impersonate_sa: The service account to impersonate for this request.

    Returns:
        A storage.Bucket object.

    Raises:
        ValueError: If the bucket name is empty.
        RuntimeError: If the storage client cannot be created or the bucket cannot be accessed.
    """
    try:
        # If an env var with this name exists, use its value; otherwise, use the name directly.
        actual_bucket_name = os.getenv(bucket_name, bucket_name)

        if not actual_bucket_name:
            raise ValueError("The bucket name cannot be empty.")

        storage_client = get_any_client("storage", target_principal=impersonate_sa)
        
        # The .bucket() method does not make an API call, so it won't raise a network error here.
        # Errors will occur when you try to perform an action on the bucket object (e.g., list_blobs).
        bucket = storage_client.bucket(actual_bucket_name, user_project=user_project)
        logger.debug(f"Successfully created handle for bucket '{actual_bucket_name}'.")
        return bucket

    except ValueError as e:
        logger.error(f"Bucket name validation failed: {e}")
        raise
    except Exception as e:
        logger.error(f"Failed to get bucket handle for '{bucket_name}': {e}")
        # Re-raise to ensure the caller knows the operation failed.
        raise RuntimeError(f"Could not get bucket handle for '{bucket_name}'.") from e
