import calendar
import pandas as pd
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
prediction = spark.read.csv("../result/predictions/", header=True, schema=prediction_schema)
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
transactions = spark.read.csv("../result/transformed_data", header=True, schema=transactions_schema)
transactions.cache()


@app.route("/api/user_predict/<user_id>")
def predict_info(user_id):
    user_predict_info = (prediction.filter(prediction["user_id"] == user_id)
                         .select("user_id", "product_id", "category_code", "brand", "price"))

    dict_list = [r.asDict() for r in user_predict_info.collect()]
    return dict_list


@app.route("/api/stat/top_category")
def top_category():
    sale_count = transactions.count()
    cat_percentage = transactions.groupBy("category_code").agg((functions.count(functions.expr("*")) / sale_count).alias("percentage"))
    cat_percentage = cat_percentage.sort("percentage", ascending=False).limit(15)
    cat_percentage = cat_percentage.withColumn("category_code",
                                               functions.element_at(functions.split(cat_percentage["category_code"], "\."), -1))
    cat_percentage = cat_percentage.cache()

    percentage_sum = cat_percentage.agg(functions.sum("percentage")).first()[0]
    other_percentage = 1 - percentage_sum

    cat_percentage_df = cat_percentage.toPandas()
    cat_percentage.unpersist()  # Remove cache to avoid OOM

    cat_percentage_df = pd.concat([cat_percentage_df,
                                   pd.DataFrame({"category_code": ["Others"], "percentage": [other_percentage]})])
    return {
        "labels": cat_percentage_df["category_code"].tolist(),
        "datasets": [{"data": cat_percentage_df["percentage"].tolist()}]
    }


@app.route("/api/stat/top_user")
def top_user():
    user_spend = transactions.groupBy("user_id").agg(
        functions.sum("price").alias("spend"),
        functions.count(functions.expr("*")).alias("count")
    )
    user_spend = user_spend.sort("spend", ascending=False).limit(10)

    top_user_df = user_spend.toPandas()

    return {
        "labels": top_user_df["user_id"].tolist(),
        "datasets": [{
            "label": "Total Spending",
            "data": top_user_df["spend"].tolist()
        }, {
            "label": "Quantity Purchased",
            "data": top_user_df["count"].tolist()
        }]
    }


def get_per_month_data(df):
    transactions_with_month = df.withColumn("month", functions.month("event_time"))
    per_month_sale = transactions_with_month.groupBy("month").agg(
        functions.sum("price").alias("sales"),
        functions.count(functions.expr("*")).alias("count")
    ).orderBy("month")
    per_month_sale_df = per_month_sale.toPandas()
    per_month_sale_df["month"] = per_month_sale_df["month"].apply(lambda x: calendar.month_name[x])

    return {
        "labels": per_month_sale_df["month"].tolist(),
        "datasets": [{
            "label": "Monthly Sales",
            "data": per_month_sale_df["sales"].tolist()
        }, {
            "label": "Quantity Sold",
            "data": per_month_sale_df["count"].tolist()
        }]
    }


@app.route("/api/stat/per_month")
def per_month():
    return get_per_month_data(transactions)


@app.route("/api/stat/top_category_per_month")
def top_category_per_month():
    category_sale_count = transactions.groupBy("category_code").agg(functions.count(functions.expr("*")).alias("count"))
    top_cat_code = category_sale_count.sort("count", ascending=False).first()["category_code"]
    transactions_top_cat = transactions.filter(transactions["category_code"] == top_cat_code)

    return get_per_month_data(transactions_top_cat)


if __name__ == "__main__":
    app.run(host='0.0.0.0')