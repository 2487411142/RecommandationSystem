import sys
from datetime import datetime

from sympy import false

assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions as sf, types, Row
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.ml.feature import StringIndexer

PATH = 's3://861276118887datasink/purchases2024/11/28/02/'


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

    # eliminate transactions happened on 1970/01/01 which means missing value
    transactions = transactions.filter(transactions['event_time'] >= datetime(2020,1,1))

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

    # clean the record if the brand is still missing
    final_data = filled_brand.filter(filled_brand['brand'].isNotNull())
    final_data.coalesce(1).write.mode('overwrite').csv('s3://861276118887datasink/transformed_data',header=True)

    # show data
    # final_data.show()

    # ALS Recommendation Model
    # find the number of purchase of items of each user
    data = final_data.groupBy(final_data.user_id, final_data.product_id).count()

    # use a string indexer to transform the (long) id to (int) id
    data = data.withColumn('user_id', data['user_id'].cast('string'))
    data = data.withColumn('product_id', data['product_id'].cast('string'))
    indexer1 = StringIndexer(inputCol="user_id", outputCol="user_id_index")
    indexer2 = StringIndexer(inputCol="product_id", outputCol="product_id_index")
    model1 =  indexer1.fit(data)
    temp = model1.transform(data)
    model2 = indexer2.fit(temp)
    ready_to_use = model2.transform(temp)
    ready_to_use = (ready_to_use.withColumn('user_id_index', ready_to_use['user_id_index'].cast('int'))
                    .withColumn('product_id_index', ready_to_use['product_id_index'].cast('int')))
    ready_to_use.show()

    # split data
    (training, test) = ready_to_use.randomSplit([0.8, 0.2])
    training.cache()
    test.cache()

    # build and train model
    als = ALS(maxIter=5, regParam=0.01, userCol="user_id_index", itemCol="product_id_index", ratingCol="count",
              coldStartStrategy="drop", implicitPrefs=True)
    model = als.fit(training)

    # evaluate the model by computing the RMSE on the test data
    predictions = model.transform(test)
    evaluator = RegressionEvaluator(metricName="rmse", labelCol="count",
                                    predictionCol="prediction")
    rmse = evaluator.evaluate(predictions)
    print("Root-mean-square error = " + str(rmse))

    # generate top 3 product recommendations for each user
    top = model.recommendForAllUsers(3)
    top = top.withColumn('recommendations', sf.explode(sf.col('recommendations')))
    top = top.withColumn('product_id_index', top['recommendations.product_id_index'])
    top = top.withColumn('rating', top['recommendations.rating'])
    top = top.drop('recommendations')
    top.show(10)

    # get product id -> product id index map
    product_map = ready_to_use.select('product_id_index', 'product_id').distinct()

    # join map to get product id
    top = top.join(product_map, on='product_id_index', how="left")
    top.show(10)

    # get user id index -> user id map
    user_map = ready_to_use.select('user_id_index', 'user_id').distinct()

    # join user_map to get user id
    top = top.join(user_map, on='user_id_index', how="left")

    # get product id -> category code & brand & price map
    id_to_code_map = final_data.select('product_id', 'category_code', 'brand', 'price').distinct()
    top = top.join(id_to_code_map, on='product_id', how="left")
    top = top.orderBy(['user_id_index', 'rating'], ascending=[True, False])
    top = top.select('user_id_index', 'user_id', 'product_id', 'rating', 'category_code', 'brand', 'price')
    top.show(10)


    top.coalesce(1).write.mode('overwrite').csv('s3://861276118887datasink/predictions',header=True)


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