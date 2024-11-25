from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("MSK_Consumer") \
    .getOrCreate()

# Kafka Configuration
kafka_servers = "b-1.msktutorialcluster.zqx1m7.c6.kafka.us-west-2.amazonaws.com:9098," \
                "b-2.msktutorialcluster.zqx1m7.c6.kafka.us-west-2.amazonaws.com:9098"

try:
    # Read from Kafka
    kafka_df = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", kafka_servers) \
        .option("subscribe", "MSKTutorialTopic") \
        .option("kafka.security.protocol", "SASL_SSL") \
        .option("kafka.sasl.mechanism", "AWS_MSK_IAM") \
        .option("kafka.sasl.jaas.config", "software.amazon.msk.auth.iam.IAMLoginModule required;") \
        .option("kafka.sasl.client.callback.handler.class", "software.amazon.msk.auth.iam.IAMClientCallbackHandler") \
        .load()

    # Transform Kafka messages to readable format
    transformed_df = kafka_df.selectExpr(
        "CAST(key AS STRING) as message_key",
        "CAST(value AS STRING) as message_value",
        "timestamp"
    )

    # Write to console (for development testing)
    query = transformed_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .start()

    # Await termination
    query.awaitTermination()

except Exception as e:
    print(f"Error: {e}")
    raise
