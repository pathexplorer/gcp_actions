# dropbox_operation.py
from google.cloud import secretmanager
from google.auth import impersonated_credentials
from google.auth.transport.requests import AuthorizedSession

# Define the email of the SA you want to use for this module
DROPBOX_SA_EMAIL = "dropbox-secrets-sa@YOUR_PROJECT_ID.iam.gserviceaccount.com"
PROJECT_ID = "YOUR_PROJECT_ID"
DROPBOX_SECRET_ID = "dropbox-api-secret"


def get_dropbox_secret(secret_id: str):
    """Accesses a secret using the specialized Dropbox Service Account."""

    # The audience must be the service you are trying to talk to (Secret Manager)
    target_audience = 'https://secretmanager.googleapis.com/'

    # 1. Generate short-lived credentials for the Dropbox SA
    impersonated_creds = impersonated_credentials.Credentials(
        source_credentials=None,  # Use default (Runtime SA) credentials
        target_account=DROPBOX_SA_EMAIL,
        target_scopes=["https://www.googleapis.com/auth/cloud-platform"],
        # The service your credentials will be used for
        delegated_scopes=[],
        target_audience=target_audience,
        # This is the maximum recommended time
        lifetime=300
    )

    # 2. Use the generated credentials to initialize the Secret Manager client
    # This client now acts *as* the Dropbox SA
    http_session = AuthorizedSession(impersonated_creds)
    client = secretmanager.SecretManagerServiceClient(credentials=impersonated_creds, transport=http_session)

    # 3. Access the secret
    name = f"projects/{PROJECT_ID}/secrets/{secret_id}/versions/latest"
    response = client.access_secret_version(request={"name": name})

    return response.payload.data.decode("UTF-8")

# Example usage:
# dropbox_config = get_dropbox_secret(DROPBOX_SECRET_ID)