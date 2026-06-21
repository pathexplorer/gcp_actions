from gcp_actions.common_utils import local_runner as lr
import logging
from gcp_actions.common_utils.timer import run_timer
from google.cloud import pubsub_v1
import json
import base64
import os

logger = logging.getLogger(__name__)

lr.check_cloud_or_local_run()


# --- Environment-Aware Publishing ---
@run_timer
def publish_message_grpc(topic_name, message_data):
    """Publishes using the modern, high-performance gRPC client (for Cloud Run)."""
    project_id = os.environ.get('GCP_PROJECT_ID')
    if not project_id:
        # Fallback for local testing or if the environment variable is missing
        logger.error("GCP_PROJECT environment variable not found. Using 'your-gcp-project-id'.")
    client = pubsub_v1.PublisherClient()
    topic_path = client.topic_path(project_id, topic_name)
    data = json.dumps(message_data).encode("utf-8")
    future = client.publish(topic_path, data)
    message_id = future.result(timeout=30)
    logger.info(f"Published message {message_id} via gRPC.")
    return message_id


@run_timer
def publish_message_https(topic_name, message_data):
    """Publishes using the stable, pure HTTPS client (for local development)."""
    project_id = os.environ.get('GCP_PROJECT_ID')
    from googleapiclient.discovery import build
    from google.auth import default
    if not project_id:
        # Fallback for local testing or if the environment variable is missing
        logger.error("GCP_PROJECT environment variable not found. Using 'your-gcp-project-id'.")
    credentials, _ = default()
    client = build('pubsub', 'v1', credentials=credentials, cache_discovery=False)

    data = base64.b64encode(json.dumps(message_data).encode('utf-8')).decode('utf-8')
    body = {'messages': [{'data': data}]}
    topic_path = f'projects/{project_id}/topics/{topic_name}'

    request = client.projects().topics().publish(topic=topic_path, body=body)
    response = request.execute()
    message_id = response['messageIds'][0]
    logger.info(f"Published message {message_id} via HTTPS.")
    return message_id


def publish_to_pubsub(topic_name, message_data):
    """ Selects the appropriate publishing method based on the environment."""
    try:
        if os.environ.get('K_SERVICE'):
            # In Cloud Run, use the fast gRPC client
            return publish_message_grpc(topic_name, message_data)
        else:
            # Locally, use the reliable HTTPS client to bypass network issues
            return publish_message_https(topic_name, message_data)
    except Exception as e:
        logger.error(f"Failed to publish message for topic '{topic_name}': {e}", exc_info=True)
        raise
