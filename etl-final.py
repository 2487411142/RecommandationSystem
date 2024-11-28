import sys

assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions as sf, types, Row
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS

PATH = '/Users/kevin/PycharmProjects/finalGit/New'

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

    # show data
    # final_data.show()



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