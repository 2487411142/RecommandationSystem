import boto3
import pandas as pd
import subprocess
from io import BytesIO

# Configuration
BUCKET_NAME = '861276118887transactions'
CSV_FILE_KEY = 'transactions.xlsx' #TODO:UPDATE TO READ CSV FILE
KAFKA_TOPIC = 'MSKTutorialTopic'
KAFKA_BROKER = 'b-1.msktutorialcluster.zqx1m7.c6.kafka.us-west-2.amazonaws.com:9098'
KAFKA_CONFIG = '/home/ec2-user/kafka_2.13-3.6.0/bin/client.properties'


def download_excel_from_s3(bucket_name, file_key):
    """Download an Excel file from S3 and return its content as a DataFrame."""
    s3 = boto3.client('s3')
    print(f"Downloading {file_key} from bucket {bucket_name}...")
    response = s3.get_object(Bucket=bucket_name, Key=file_key)
    file_content = response['Body'].read()  # Read the file content as binary
    return pd.read_excel(BytesIO(file_content))  # Load it into a Pandas DataFrame


def send_dataframe_to_kafka(df):
    """Send each row of the DataFrame as JSON to the Kafka topic."""
    for _, row in df.iterrows():
        # Convert the row to JSON
        json_data = row.to_json()
        print(f"Sending message: {json_data}")

        # Use Kafka CLI to send the message
        process = subprocess.Popen(
            [
                "/home/ec2-user/kafka_2.13-3.6.0/bin/kafka-console-producer.sh",
                "--broker-list", KAFKA_BROKER,
                "--producer.config", KAFKA_CONFIG,
                "--topic", KAFKA_TOPIC
            ],
            stdin=subprocess.PIPE
        )
        # Send the JSON data
        process.communicate(input=json_data.encode())


def main():
    # Download the Excel file from S3 and load it as a DataFrame
    df = download_excel_from_s3(BUCKET_NAME, CSV_FILE_KEY)

    # Send the DataFrame content to Kafka as JSON
    send_dataframe_to_kafka(df)

    print("All messages sent successfully!")


if __name__ == "__main__":
    main()
