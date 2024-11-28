# BigDataFinalProject

dataset link: https://www.kaggle.com/datasets/kartik2112/fraud-detection

create Kafka: https://docs.aws.amazon.com/msk/latest/developerguide/create-cluster.html

Work:
1.	Load local file to S3 bucket
2.	In ec2 transform csv file to json messages and pass messages to Kafka 
3.	Use EMR Spark consume(etl), save transformed messages back to S3.
4.	Use sageMaker to build model and predict. The model accepts name and recommend a product. Save the result to RDS.
5.	Visualize in local. 


# More data analysis requirements
1. The event_time column may contain outdated entries, such as dates from '1970'. 
   To ensure the data is reasonable, we need to remove these old time.


2. We need to consider calculating which extra useful information:
a. Calculate how long it has been since each customer last made a purchase relative to a specified date.
b. Count the number of transactions for each customer.
c. Calculate the total transaction amount for each customer.
