from google.cloud import firestore
from service_helpers import local_runner as lr

lr.check_cloud_or_local_run()

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

if __name__ == "__main__":
    val = check_swith_status()
    print(val)
