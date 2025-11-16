from google.cloud import storage
from functools import lru_cache
import os
from dotenv import load_dotenv

IS_LOCAL = os.environ.get("K_SERVICE") is None

if IS_LOCAL: # then load .env file
    dotenv_path = os.path.join(os.path.dirname(__file__), "../project_env/keys.env")
    load_dotenv(dotenv_path=dotenv_path, override=False)
    print("local env loaded")

@lru_cache(maxsize=1)
def get_env_and_cashed_it(variable: str):
    """
    Loads and caches variable from the environment
    :param variable: "GCP_PROJECT_ID" or gcp_project_id (which == "GCP_PROJECT_ID")
    """
    get_variable = os.getenv(variable)
    if not get_variable:
        raise EnvironmentError(f"{get_variable} environment variable not set. Please configure your environment.")
    return get_variable

@lru_cache(maxsize=1)
def get_client():
    return storage.Client()

@lru_cache(maxsize=2)
def get_bucket(bucket_name: str ):
    """
    :param bucket_name: variable GCS_BUCKET_NAME or GCS_PUBLIC_BUCKET
    :return:
    """
    actual_bucket_name = get_env_and_cashed_it(bucket_name)
    return get_client().bucket(actual_bucket_name)



