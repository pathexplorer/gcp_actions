from gcp_actions.client import get_any_client
from gcp_actions.firestore_box.json_manipulations import FirestoreMagic
def create_in_firestore():

    db = get_any_client("firestore")

    # Path: config/front-side-for-friends/settings/data
    doc_ref = db.collection('config').document('shared').collection('settings').document('data')

    # Your configuration data
    # config_data = {
    #     "max_upload_size": 10485760,  # 10 MB
    #     "allowed_file_types": ["fit", "gpx"],
    #     "feature_flags": {
    #         "email_notifications": True,
    #         "advanced_analytics": False
    #     },
    #     "debug_mode": False
    config_data = {
    "CS_BUCKET_NAME": "jetpack-multifunctional-nightbirds-9n9c1k",
    "EMAIL_MODE": "brevo",
    "GCS_PUB_INPUT_BUCKET": "world-riders-possible-barn-w5a7o9",
    "GCS_PUB_OUTPUT_BUCKET": "finegood-helpers-pure-and-use-w6d2k8"
    }


    doc_ref.set(config_data)
    print("✅ Configuration document created successfully!")
#
# if __name__ == "__main__":
    # fs = FirestoreMagic("firestore", "telegram")
    # data = {"test": "test"}
    #
    #
    # fs.create_firejson("keywords", data)