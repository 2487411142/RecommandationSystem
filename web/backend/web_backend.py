from flask import Flask
from pyspark.sql import SparkSession, functions, types

app = Flask(__name__)
spark = SparkSession.builder.appName("Website Backend").getOrCreate()

prediction_schema = types.StructType([
    types.StructField("user_id_index", types.LongType()),
    types.StructField("user_id", types.LongType()),
    types.StructField("product_id", types.LongType()),
    types.StructField("rating", types.DoubleType()),
    types.StructField("category_code", types.StringType()),
    types.StructField("brand", types.StringType()),
    types.StructField("price", types.DoubleType()),
])
prediction = spark.read.csv("../../result/predictions/", header=True, schema=prediction_schema)
prediction.cache()

transactions_schema = types.StructType([
    types.StructField("event_time", types.TimestampType()),
    types.StructField("order_id", types.LongType()),
    types.StructField("product_id", types.LongType()),
    types.StructField("category_id", types.LongType()),
    types.StructField("category_code", types.StringType()),
    types.StructField("brand", types.StringType()),
    types.StructField("price", types.FloatType()),
    types.StructField("user_id", types.LongType())
])
transactions = spark.read.csv("../../result/transformed_data", header=True, schema=transactions_schema)


@app.route("/api/user_predict/<user_id>")
def predict_info(user_id):
    user_predict_info = (prediction.filter(prediction["user_id"] == user_id)
                         .select("user_id", "product_id", "category_code", "brand", "price")
                         .collect())
    user_predict = [r.asDict() for r in user_predict_info]
    return user_predict


if __name__ == "__main__":
    app.run(host='0.0.0.0')