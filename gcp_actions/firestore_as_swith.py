from dotenv import load_dotenv
import os
from google.cloud import firestore


IS_LOCAL = os.environ.get("K_SERVICE") is None

if IS_LOCAL: # then load .env file
    dotenv_path = os.path.join(os.path.dirname(__file__), "/home/stas/Dropbox/projects/gcp_actions_ns/gcp_actions/local_test/gcp.env")
    load_dotenv(dotenv_path=dotenv_path, override=False)



db = firestore.Client()

def check_swith_status():
    # Read the state on every call (or cache it periodically)
    flag_doc = db.collection("config").document("feature_flags").get()

    if flag_doc.exists and flag_doc.to_dict().get("data_processing_enabled"):
        print("Mode is ON. Executing function.")
        return "prod"
    else:
        print("Mode is OFF. Skipping function.")
        return "testing"

# val = check_swith_status()
# print(val)
