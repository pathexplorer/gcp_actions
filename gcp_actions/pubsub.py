import json
import os
from google.cloud import pubsub_v1
from gcp_actions.common_utils import local_runner as lr
import logging

logger = logging.getLogger(__name__)

lr.check_cloud_or_local_run()

def publish_message(topic_name: str, message_data: dict):
    """
    Publishes a message to a Pub/Subtopic to trigger backend processing.

    Args:
        topic_name: The name of the PubSub topic (e.g., 'fit-file-processing-topic').
        message_data: A dictionary containing the file path, email, etc.
    """
    # The PubSub topic name should be prefixed with the project path for Global Services

    project_id = os.environ.get('GCP_PROJECT_ID')

    if not project_id:
        # Fallback for local testing or if the environment variable is missing
        logger.error("GCP_PROJECT environment variable not found. Using 'your-gcp-project-id'.")


    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_name)

    # The message must be a byte string (JSON encoded)
    try:
        data_json = json.dumps(message_data)
        data_bytes = data_json.encode("utf-8")

        # Publish the message
        future = publisher.publish(topic_path, data=data_bytes)

        # This line blocks until the publishing is complete (useful for immediate feedback)
        message_id = future.result()

        logger.info(f"✅ Published message ID: {message_id}")

    except Exception as e:
        logger.error(f"Failed to publish message to {topic_path}.")
        logger.error(f"Data attempted: {message_data}")
        raise RuntimeError(f"Pub/Sub failed: {e}")