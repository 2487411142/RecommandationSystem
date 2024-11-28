import sys

assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions as sf, types, Row
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS

PATH = '/Users/kevin/PycharmProjects/finalGit/861276118887datasink/transactions2024/11/27/23'

def main():
    # read rdd
    transactions = spark.read.json(PATH, schema= transactions_schema)
    transactions = transactions.distinct()

    # cast id
    transactions = transactions.withColumn('order_id', transactions['order_id'].cast('long'))
    transactions = transactions.withColumn('product_id', transactions['product_id'].cast('long'))
    transactions = transactions.withColumn('category_id', transactions['category_id'].cast('long'))
    transactions = transactions.withColumn('price', transactions['price'].cast('float'))
    transactions = transactions.withColumn('user_id', transactions['user_id'].cast('long'))

    # '' -> None
    transactions = transactions.withColumn('category_code',
                                           sf.when(transactions['category_code'] == '', None)
                                           .otherwise('category_code'))
    transactions = transactions.withColumn('brand', sf.when(transactions['brand'] == '', None)
                                           .otherwise('brand'))

    # eliminate transactions without userid
    transactions = transactions.filter(transactions['user_id'].isNotNull())

    from pyspark.ml.evaluation import RegressionEvaluator
    from pyspark.ml.recommendation import ALS




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