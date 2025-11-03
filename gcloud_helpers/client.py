from google.cloud import storage
from functools import lru_cache
import os

@lru_cache(maxsize=1)
def get_bucket_name():
    return os.getenv("GCS_BUCKET_NAME")

@lru_cache(maxsize=1)
def get_client():
    return storage.Client()

@lru_cache(maxsize=1)
def get_bucket():
    return get_client().bucket(get_bucket_name())
