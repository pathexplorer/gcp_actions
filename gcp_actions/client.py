from google.cloud import storage
from functools import lru_cache
import os
from dotenv import load_dotenv

dotenv_path = os.path.join(os.path.dirname(__file__), "../local_test/gcp.env")
load_dotenv(dotenv_path=dotenv_path, override=False)

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

@lru_cache(maxsize=1)
def get_bucket():
    return get_client().bucket(get_env_and_cashed_it("GCS_BUCKET_NAME"))

