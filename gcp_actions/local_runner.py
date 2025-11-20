import os
from dotenv import load_dotenv

def check_cloud_or_local_run():

    is_local = os.environ.get("K_SERVICE") is None

    if is_local:  # then load .env file
        dotenv_path = os.path.join(os.path.dirname(__file__), "../local_test/gcp.env")
        load_dotenv(dotenv_path=dotenv_path, override=False)
        print("Local running")

if __name__ == "__main__":
    check_cloud_or_local_run()