from google.cloud import storage
from functools import lru_cache
import os
from dotenv import load_dotenv

IS_LOCAL = os.environ.get("K_SERVICE") is None

if IS_LOCAL:  # then load .env file
    dotenv_path = os.path.join(os.path.dirname(__file__), "../project_env/keys.env")
    load_dotenv(dotenv_path=dotenv_path, override=False)
    print("local env loaded")


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
def get_client():
    return storage.Client()


@lru_cache(maxsize=8)  # Increased cache for different project/bucket combos
def get_bucket(bucket_name: str, user_project: str | None = None):
    """
    Gets a bucket handle, supporting Requester Pays.
    It first tries to resolve bucket_name as an environment variable.
    If that fails, it assumes bucket_name is the literal name.

    :param bucket_name: An env var name (e.g., "GCS_BUCKET_NAME") or the literal bucket name.
    :param user_project: The project ID to bill for Requester Pays requests.
    :return: A Google Cloud Storage Bucket object.
    """
    # Try to get the bucket name from an environment variable.
    # os.getenv() returns None if the variable doesn't exist.
    actual_bucket_name = os.getenv(bucket_name)

    # If the environment variable wasn't found, assume the provided
    # bucket_name is the actual name.
    if not actual_bucket_name:
        actual_bucket_name = bucket_name

    if not actual_bucket_name:
        # This would only happen if an empty string was passed in.
        raise ValueError("The bucket name cannot be empty.")

    return get_client().bucket(actual_bucket_name, user_project=user_project)
