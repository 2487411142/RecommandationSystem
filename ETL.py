import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
from pyspark.ml.feature import StringIndexer
from pyspark.sql.functions import current_date, year
# add more functions as necessary

def main():
    # main logic starts here
    transactions_schema = types.StructType([
        types.StructField('trans_date_trans_time', types.TimestampType()),
        types.StructField('cc_num', types.StringType()),
        types.StructField('merchant', types.StringType()),
        types.StructField('category', types.StringType()),
        types.StructField('amt', types.StringType()),
        types.StructField('first', types.StringType()),
        types.StructField('last', types.StringType()),
        types.StructField('gender', types.StringType()),
        types.StructField('city', types.StringType()),
        types.StructField('state', types.StringType()),
        types.StructField('zip', types.StringType()),
        types.StructField('city_pop', types.StringType()),
        types.StructField('job', types.StringType()),
        types.StructField('dob', types.TimestampType()),
        types.StructField('trans_num', types.StringType()),
        types.StructField('unix_time', types.StringType()),
    ])

    # TODO:UPDATE FILE TO STRAMING SOURCE
    transactions = spark.read.csv(r'C:\Users\HowieGuan\PycharmProjects\BigDataFinalProject\transactions.gz',schema=transactions_schema,header=True)

    # apply stringIndexer to gender
    indexer = StringIndexer(inputCol='gender',outputCol='gender_index')
    transactions = indexer.fit(transactions).transform(transactions).drop('gender')
    transactions = transactions.withColumnRenamed('gender_index','gender')

    # get a person's age
    transactions = transactions.withColumn('age',year(current_date()) - year(functions.col('dob')))
    transactions = transactions.drop('dob')

    # TODO: WRITE TO S3

    transactions.show()

if __name__ == '__main__':
    spark = SparkSession.builder.appName('transactions etl').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main()