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


# About Visualization
For this part, using React.js as frontend framework to show variours charts, Flask as backend to deal with data.
how to run this part code. 
1. Clone the 'full_stack' flie to local;
2. cd full_stack/front_end, run the following command to install the necessary React dependencies:
   npm install
3. Once the dependencies are installed, run the React development server.
   npm start
4. Navigate to the Backend Folder， open a new terminal window and navigate to backend folder: cd full_stack/back_end
5. Install Backend Dependencies, ensure that inside the virtual environment
6. install the required Python dependencies. Using a requirements.txt file for the backend, run the following command: 
   pip install -r requirements.txt
7. If don't have a requirements.txt, please using flask and flask-cors directly, it can manually install the required dependencies with:
   pip install flask flask-cors
8. Then start the backend file: python app.py







