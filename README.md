# Introduction
Online shopping has become one of the most popular methods of shopping, generating millions
of purchases every day. Consumers can easily purchase items such as clothing, electronics,
furniture, and more from online retail platforms. One significant advantage of online retailing
over traditional in-store shopping is its ability to collect user data. While a single purchase may
offer limited insights, the aggregation of vast amounts of purchase data can uncover valuable
business trends and patterns. In this project, we analyzed a group of individuals' purchasing
preferences, employed collaborative filtering techniques to develop a recommender system, and
provided personalized recommendations.

# Methodology

This recommendation system we developed integrates cloud technology, data
streaming, data processing techniques, machine learning algorithms, and data visualization. All
cloud-related components are hosted within a unified VPC network to mitigate security risks and
minimize the overhead associated with infrastructure configuration, such as inter-component
communication. The system architecture diagram below provides an overview of the system's
workflow, accompanied by a detailed explanation of its components and functionality.


![Alt text](/structure.png?raw=true "System Architecture Diagram")

1. The above-mentioned dataset is first stored as raw data in CSV format without any
preprocessing in an ***AWS EC2*** machine.

2. A Python script is written to read the raw data and convert each row of the raw data to a
JSON format message, and finally send JSON message to an Amazon Managed
Streaming for ***Apache Kafka (MSK)*** topic to fake a streaming data source. The scalability
of MSK makes it a perfect fit for big data projects and is also the major reason for us to
choose it.

3. An ***AWS Firehose*** connector is configured on MSK to connect the MSK cluster and an
***S3*** bucket.

4. Once the messages arrive at the Kafka brokers, the Firehose connector collects them
and uploads them to an S3 bucket as a file, triggered when the data reaches a 5 MB
threshold.

5. An ***Amazon Elastic MapReduce (EMR)*** cluster processes the files stored in the S3
bucket as input. This cluster is used for both data preprocessing and collaborative
filtering-based machine learning tasks. EMR was chosen due to its built-in support for
Apache Spark, which provides a robust environment for both data processing and
machine learning through its integrated libraries. This allows us to perform both tasks
seamlessly within a single platform.

6. The ***Spark*** results which include transformed data and prediction results are stored in the
S3 bucket again.

7. A locally hosted webpage built with ***React*** and ***Flask*** reads those results to visualize the
statistics and prediction results. Apart from visualizing data, this webpage also offers a
search feature, which takes a user’s ID as input, and returns the top 3 recommendations
for the user.

# Results

From the outcomes, our system successfully identified the best products to recommend. When
a user_id was entered into the web form, the system generated personalized product
recommendations. Additionally, we extracted valuable insights from the data, such as monthly
sales trends and a list of the top ten customers based on spending, which is important for us to
do analysis and predict. From the data analysis, we learned data-handling techniques, such as
filling in missing values classifying data into meaningful categories, and using categorizing
methods to predict products. 

**There is also a 1-min [demo](732-proj-web-demo.mp4) to download.**

![Alt text](/main_page_with_recommandation.png?raw=true)

![Alt text](/visualization_page.png?raw=true)


# Links

dataset link: https://www.kaggle.com/datasets/mkechinov/ecommerce-purchase-history-from-electronics-store

create Kafka: https://docs.aws.amazon.com/msk/latest/developerguide/create-cluster.html

# About Visualization
For this part, using React.js as frontend framework to show various charts, Flask as backend to deal with data.

To run the server, you need have the docker installed.

1. Change to `web` folder
2. Execute the command:
    ```bash
    docker compose up
    ```
3. Open your browser and visit: http://localhost/

Please refer to [readme](web/README.md) in `web` folder for details about web server.

# File description
**Kafka_messages**: This folder stores all the Kafka messages, this is just for your reference, the original copy is on AWS S3.

**result**: This folder stores transformed kafka messages and the prediction result.

**web**: This folder stores web frontend and backend code.

**ML_time_wighted.py**: This is our first version of the Spark ETL code with ML. It does not perform well, but we are providing it here for your reference.

**ml-count.py**: This is our final version of the Spark ETL code with ML. 

**messagePublisher.py**: This code read in csv file and publish messages to kafka topic.


# Kafka info & Commands

MSK VPC: `vpc-0bbc25d4942c9a443`

MSK Security groups associated with VPC: `sg-09734f8fd9d3cfb9f`

MSK Subnets: `subnet-0cad84146e7564e88;subnet-0c18b130d53b4746d;subnet-01b7df6fe673bf7ed`

Topic Name: `myTopic`

create topic:
```bash
/home/ubuntu/kafka_2.13-3.6.0/bin/kafka-topics.sh --create --bootstrap-server boot-kic5gwhr.c2.kafka-serverless.us-east-1.amazonaws.com:9098 --command-config /home/ubuntu/kafka_2.13-3.6.0/bin/client.properties --replication-factor 3 --partitions 1 --topic mytopic
```
send message:
```bash
/home/ubuntu/kafka_2.13-3.6.0/bin/kafka-console-producer.sh --broker-list boot-kic5gwhr.c2.kafka-serverless.us-east-1.amazonaws.com:9098 --producer.config /home/ubuntu/kafka_2.13-3.6.0/bin/client.properties --topic myTopic
```
receive message:
```bash
/home/ubuntu/kafka_2.13-3.6.0/bin/kafka-console-consumer.sh --bootstrap-server boot-kic5gwhr.c2.kafka-serverless.us-east-1.amazonaws.com:9098 --consumer.config /home/ubuntu/kafka_2.13-3.6.0/bin/client.properties --topic mytopic
```
delete topic:
```bash
/home/ubuntu/kafka_2.13-3.6.0/bin/kafka-topics.sh --delete --bootstrap-server boot-kic5gwhr.c2.kafka-serverless.us-east-1.amazonaws.com:9098 --topic mytopic
```

