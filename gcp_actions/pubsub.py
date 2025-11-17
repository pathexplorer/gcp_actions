import json # Added for Pub/Sub message encoding
import os
from google.cloud import pubsub_v1

def publish_message(topic_name: str, message_data: dict):
    """
    Publishes a message to a Pub/Sub topic to trigger backend processing.

    Args:
        topic_name: The name of the Pub/Sub topic (e.g., 'fit-file-processing-topic').
        message_data: A dictionary containing the file path, email, etc.
    """
    # The Pub/Sub topic name should be prefixed with the project path for Global Services
    project_id = os.environ.get('GCP_PROJECT_ID')  # Assuming GCP_PROJECT is set by Cloud Run

    if not project_id:
        # Fallback for local testing or if environment variable is missing
        print("Warning: GCP_PROJECT environment variable not found. Using 'your-gcp-project-id'.")
        project_id = 'your-gcp-project-id'

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_name)

    # The message must be a byte string (JSON encoded)
    try:
        data_json = json.dumps(message_data)
        data_bytes = data_json.encode("utf-8")

        # Publish the message
        future = publisher.publish(topic_path, data=data_bytes)

        # This line blocks until the publish is complete (useful for immediate feedback)
        message_id = future.result()

        print(f"✅ Published message ID: {message_id}")

    except Exception as e:
        print(f"   Pub/Sub Publish ERROR: Failed to publish message to {topic_path}.")
        print(f"   Data attempted: {message_data}")
        raise RuntimeError(f"Pub/Sub failed: {e}")