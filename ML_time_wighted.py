import sys

from pyspark.ml.feature import StringIndexer

assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions as sf, types, Row
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.sql.functions import current_date, year,datediff
from pyspark.sql.types import IntegerType


PATH = './New'

def main():
    # read rdd
    transactions = spark.read.json(PATH, schema= transactions_schema)
    transactions = transactions.distinct()

    # cast (string) id -> (long) id
    transactions = transactions.withColumn('order_id', transactions['order_id'].cast('long'))
    transactions = transactions.withColumn('product_id', transactions['product_id'].cast('long'))
    transactions = transactions.withColumn('category_id', transactions['category_id'].cast('long'))
    transactions = transactions.withColumn('price', transactions['price'].cast('float'))
    transactions = transactions.withColumn('user_id', transactions['user_id'].cast('long'))

    # '' -> None
    transactions = transactions.withColumn('category_code',
                                           sf.when(transactions['category_code'] != '', transactions['category_code']))
    transactions = transactions.withColumn('brand', sf.when(transactions['brand'] != '', transactions['brand']))

    # eliminate transactions without userid
    transactions = transactions.filter(transactions['user_id'].isNotNull())

    # cache
    transactions.cache()
    # print('first elimination')
    # print(transactions.filter(transactions['brand'] == '').count())
    # transactions.show()


    # finding the map between category_id, category_code
    category_mapping = (transactions.select("category_id", "category_code")
                        .filter(sf.col("category_code").isNotNull())
                        .dropDuplicates()
                        )

    # filling the missing category code
    filled_category = (
        transactions
        .join(category_mapping.withColumnRenamed("category_code", "category_code_mapping"), on="category_id",
              how="left")
        .withColumn(
            "category_code",
            sf.coalesce(sf.col("category_code_mapping"), sf.lit(None))
        )
        .drop("category_code_mapping")
    )

    # eliminate the rows whose category code is still empty
    filled_category= filled_category.filter(sf.col("category_code").isNotNull())
    # print('map category codes')
    # print(filled_category.filter(filled_category['category_code'].isNull()).count())
    # filled_category.show()
    filled_category.cache()

    # find the map between product_id, brand
    brand_mapping = (
        filled_category.groupBy("product_id")
        .agg(sf.first("brand", ignorenulls=True).alias("brand_mapping_temp"))
    )

    # fill the missing brand
    filled_brand = (
        filled_category
        .join(
            brand_mapping,
            on=["product_id"],
            how="left"
        )
        .withColumn(
            "brand",
            sf.coalesce(sf.col("brand"), sf.col(
                "brand_mapping_temp"), sf.lit(None))
        )
        .drop("brand_mapping_temp")
    )

    # cleaning up the whole category_code
    final_data = filled_brand.filter(filled_brand['brand'].isNotNull())
    final_data.cache()
    # show data
    # final_data.show()

    # convert product_id to integer number to prevent overflow
    indexer = StringIndexer(inputCol='product_id',outputCol='product_id_index')
    final_data = indexer.fit(final_data).transform(final_data)
    final_data = final_data.withColumn('product_id_index', final_data['product_id_index'].cast(IntegerType()))

    # convert user_id to integer number to prevent overflow
    indexer = StringIndexer(inputCol='user_id',outputCol='user_id_index')
    final_data = indexer.fit(final_data).transform(final_data)
    final_data = final_data.withColumn('user_id_index', final_data['user_id_index'].cast(IntegerType()))

    final_data.show()

    # assign weight based on the purchase time
    time_weighted = final_data.withColumn(
        'time_weight',
        sf.when(datediff(sf.lit('2020-11-18'), sf.col('event_time')) <= 90, 15)
        .when((datediff(sf.lit('2020-11-18'), sf.col('event_time')) > 90) & (
                    datediff(sf.lit('2020-11-18'), sf.col('event_time')) <= 180), 10)
        .when((datediff(sf.lit('2020-11-18'), sf.col('event_time')) > 180) & (
                datediff(sf.lit('2020-11-18'), sf.col('event_time')) <= 360), 5)
        .otherwise(0)
    )
    time_weighted.cache()

    # add up weight from the same user on the same product => get a user's preferences on a product
    time_weight_sum = time_weighted.groupBy('product_id_index','user_id_index').agg(sf.sum('time_weight').alias('time_weight_sum'))
    time_weight_sum.show()

    (training, test) = time_weight_sum.randomSplit([0.8, 0.2])

    # Build the recommendation model using ALS on the training data
    # Note we set cold start strategy to 'drop' to ensure we don't get NaN evaluation metrics
    als = ALS(maxIter=5, regParam=0.01, implicitPrefs=True,userCol="user_id_index", itemCol="product_id_index", ratingCol="time_weight_sum",
              coldStartStrategy="drop")
    model = als.fit(training)

    # Evaluate the model by computing the RMSE on the test data
    predictions = model.transform(test)
    evaluator = RegressionEvaluator(metricName="rmse", labelCol="time_weight_sum",
                                    predictionCol="prediction")
    rmse = evaluator.evaluate(predictions)
    print("Root-mean-square error = " + str(rmse))

    # Generate top 10 movie recommendations for each user
    userRecs = model.recommendForAllUsers(3)
    userRecs.show()

    # Generate top 10 user recommendations for each movie
    prodRecs = model.recommendForAllItems(10)

    # Generate top 10 movie recommendations for a specified set of users
    users = time_weight_sum.select(als.getUserCol()).distinct().limit(3)
    userSubsetRecs = model.recommendForUserSubset(users, 10)
    # Generate top 10 user recommendations for a specified set of movies
    movies = time_weight_sum.select(als.getItemCol()).distinct().limit(3)
    prodSubSetRecs = model.recommendForItemSubset(movies, 10)















if __name__ == '__main__':
    spark = SparkSession.builder.appName('purchase history').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    transactions_schema = types.StructType([
        types.StructField('event_time', types.TimestampType()),
        types.StructField('order_id', types.StringType()),
        types.StructField('product_id', types.StringType()),
        types.StructField('category_id', types.StringType()),
        types.StructField('category_code', types.StringType()),
        types.StructField('brand', types.StringType()),
        types.StructField('price', types.StringType()),
        types.StructField('user_id', types.StringType())
    ])
    main()