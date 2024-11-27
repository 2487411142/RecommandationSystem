from kafka import KafkaProducer
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
import json
import csv

# Kafka Configuration
TOPIC_NAME = 'bigdataProject'
BROKERS = 'boot-kic5gwhr.c2.kafka-serverless.us-east-1.amazonaws.com:9098'
REGION = 'us-east-1'

class MSKTokenProvider():
    def token(self):
        token, _ = MSKAuthTokenProvider.generate_auth_token(REGION)
        return token

tp = MSKTokenProvider()

producer = KafkaProducer(
    bootstrap_servers=BROKERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    retry_backoff_ms=500,
    request_timeout_ms=20000,
    security_protocol='SASL_SSL',
    sasl_mechanism='OAUTHBEARER',
    sasl_oauth_token_provider=tp, )

def read_csv_chunk(csv_file_path,batch_size):
    try:
        with open(csv_file_path, mode='r', encoding='utf-8-sig') as file:
            reader = csv.DictReader(file)
            batch = []

            for i, row in enumerate(reader):
                batch.append(row)

                # When batch is full, publish to Kafka
                if len(batch) >= batch_size:
                    publish_batch(batch)
                    batch = []  # Clear batch buffer

                # Log progress every 10,000 rows
                if i % batch_size == 0:
                    print(f"{i} rows processed...")

            # Publish remaining rows in batch
            if batch:
                publish_batch(batch)

    except Exception as e:
        print(f"Error processing file: {e}")

    finally:
        # Flush all pending messages
        producer.flush()

def publish_batch(batch):
    """Publish a batch of messages to Kafka."""
    for row in batch:
        try:
            producer.send(TOPIC_NAME, value=row)
        except Exception as e:
            print(f"Error sending message: {e}")

if __name__ == '__main__':

    csv_file_path = 'purchase_history_from_electronics_store.csv'      # Path to CSV file
    batch_size = 10000              # message batch size

    read_csv_chunk(csv_file_path,batch_size) # Process the CSV file in batch and publish to Kafka