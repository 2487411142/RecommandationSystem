import boto3
import pandas as pd
import json
from confluent_kafka import Producer

# Kafka configuration
KAFKA_BROKER = 'b-1.msktutorialcluster.zqx1m7.c6.kafka.us-west-2.amazonaws.com:9098'
TOPIC_NAME = 'MSKTutorialTopic'
PRODUCER_CONFIG = {
    'bootstrap.servers': KAFKA_BROKER,
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'AWS_MSK_IAM',
    'sasl.jaas.config': 'software.amazon.msk.auth.iam.IAMLoginModule required;',
    'sasl.client.callback.handler.class': 'software.amazon.msk.auth.iam.IAMClientCallbackHandler'
}

# S3 configuration
BUCKET_NAME = 'your-s3-bucket-name'
CSV_FILE_KEY = 'path/to/your-file.csv'


def download_csv_from_s3(bucket_name, file_key):
    """Download a CSV file from S3."""
    s3 = boto3.client('s3')
    print(f"Downloading {file_key} from bucket {bucket_name}...")
    response = s3.get_object(Bucket=bucket_name, Key=file_key)
    data = response['Body'].read().decode('utf-8')
    return pd.read_csv(pd.compat.StringIO(data))  # Load CSV into a DataFrame


def send_to_kafka(producer, topic, message):
    """Send a message to Kafka."""
    producer.produce(topic, value=message)
    producer.flush()  # Ensure the message is sent before proceeding


def main():
    # Download CSV from S3
    csv_data = download_csv_from_s3(BUCKET_NAME, CSV_FILE_KEY)

    # Initialize Kafka producer
    producer = Producer(PRODUCER_CONFIG)

    # Convert each row of the CSV to JSON and send to Kafka
    for _, row in csv_data.iterrows():
        # Convert row to JSON
        message = row.to_json()
        print(f"Sending message: {message}")

        # Send message to Kafka
        send_to_kafka(producer, TOPIC_NAME, message)

    print("All messages sent successfully!")


if __name__ == "__main__":
    main()
