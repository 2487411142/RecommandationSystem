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
    types.StructField("price", types.DoubleType()),
    types.StructField("user_id", types.LongType())
])
transactions = spark.read.csv("../../result/transformed_data", header=True, schema=transactions_schema)
transactions.cache()


@app.route("/api/user_predict/<user_id>")
def predict_info(user_id):
    user_predict_info = (prediction.filter(prediction["user_id"] == user_id)
                         .select("user_id", "product_id", "category_code", "brand", "price")
                         .collect())
    user_predict = [r.asDict() for r in user_predict_info]
    return user_predict


@app.route("/api/stat/top_category")
def top_category():
    sale_count = transactions.count()
    cat_percentage = transactions.groupBy("category_code").agg((functions.count(functions.expr("*")) / sale_count).alias("percentage"))
    cat_percentage = cat_percentage.sort("percentage", ascending=False).limit(15)
    cat_percentage = cat_percentage.cache()

    percentage_sum = cat_percentage.agg(functions.sum("percentage")).first()[0]
    other_percentage = 1 - percentage_sum

    cat_percentage_list = [r.asDict() for r in cat_percentage.collect()]
    cat_percentage_list.append({"category_code": "Other", "percentage": other_percentage})

    cat_percentage.unpersist()  # Remove cache to avoid OOM
    return cat_percentage_list


@app.route("/api/stat/top_user")
def top_user():
    user_spend = transactions.groupBy("user_id").agg(
        functions.sum("price").alias("spend"),
        functions.count(functions.expr("*")).alias("count")
    )
    user_spend = user_spend.sort("spend", ascending=False).limit(10)

    user_spend_list = [r.asDict() for r in user_spend.collect()]
    return user_spend_list


def get_per_month_data(df):
    transactions_with_month = df.withColumn("month", functions.month("event_time"))
    per_month_sale = transactions_with_month.groupBy("month").agg(
        functions.sum("price").alias("sales"),
        functions.count(functions.expr("*")).alias("count")
    ).orderBy("month")
    per_month_sale = per_month_sale.withColumn(
        "month",
        functions.when(per_month_sale["month"] == 1, "January")
                 .when(per_month_sale["month"] == 2, "February")
                 .when(per_month_sale["month"] == 3, "March")
                 .when(per_month_sale["month"] == 4, "April")
                 .when(per_month_sale["month"] == 5, "May")
                 .when(per_month_sale["month"] == 6, "June")
                 .when(per_month_sale["month"] == 7, "July")
                 .when(per_month_sale["month"] == 8, "August")
                 .when(per_month_sale["month"] == 9, "September")
                 .when(per_month_sale["month"] == 10, "October")
                 .when(per_month_sale["month"] == 11, "November")
                 .when(per_month_sale["month"] == 12, "December")
                 .otherwise("Unknown")
    )

    per_month_sale_list = [r.asDict() for r in per_month_sale.collect()]
    return per_month_sale_list


@app.route("/api/stat/per_month")
def per_month():
    return get_per_month_data(transactions)


@app.route("/api/stat/top_user_per_month")
def top_user_per_month():
    user_spend = transactions.groupBy("user_id").agg(functions.sum("price").alias("spend"))
    top_user_id = user_spend.sort("spend", ascending=False).first()["user_id"]
    transactions_top_user = transactions.filter(transactions["user_id"] == top_user_id)

    return get_per_month_data(transactions_top_user)


if __name__ == "__main__":
    app.run(host='0.0.0.0')