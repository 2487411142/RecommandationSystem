# BigDataFinalProject

dataset link: https://www.kaggle.com/datasets/kartik2112/fraud-detection

create Kafka: https://docs.aws.amazon.com/msk/latest/developerguide/create-cluster.html

Work:
1.	Load local file to S3 bucket
2.	In ec2 transform csv file to json messages and pass messages to Kafka 
3.	Use EMR Spark consume(etl), save transformed messages back to S3.
4.	Use sageMaker to build model and predict. The model accepts name and recommend a product. Save the result to RDS.
5.	Visualize in local. 


