import sys
from pyspark.sql import SparkSession, functions as F, types
from pyspark.sql.functions import current_date, year
from pyspark.sql.functions import to_timestamp


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


def main(input_paths):
    data = spark.read.json(input_paths, schema=transactions_schema)
    # remove duplicate data
    data = data.distinct()

    # removing the user_id is missing value
    new_data = data.filter(data['user_id'].isNotNull())

    # Finding the relationship bewteen category_id, category_code
    category_mapping = (new_data.select("category_id", "category_code")
                        .filter(F.col("category_code").isNotNull())
                        .dropDuplicates()

                        )
    # filling the category_code that is missing value
    filled_catetory = (
        new_data
        .join(category_mapping.withColumnRenamed("category_code", "category_code_mapping"), on="category_id", how="left")
        .withColumn(
            "category_code",
            F.coalesce(F.col("category_code_mapping"), F.lit("na"))
        )
        .drop("category_code_mapping")
    )
    # cleaning up the whole category_code
    data_cleaned = filled_catetory.filter(F.col("category_code") != "na")

    # Finding the relationship bewteen product_id, brand
    brand_mapping = (
        data_cleaned.groupBy("product_id")
        .agg(F.first("brand", ignorenulls=True).alias("brand_mapping"))
    )

    # filling the brand that is missing value
    filled_brand = (
        data_cleaned
        .join(
            brand_mapping.withColumnRenamed(
                "brand_mapping", "brand_mapping_temp"),
            on=["product_id", "price"],
            how="left"
        )
        .withColumn(
            "brand",
            F.coalesce(F.col("brand"), F.col(
                "brand_mapping_temp"), F.lit("na"))
        )
        .drop("brand_mapping_temp")
    )

    # cleaning up the whole category_code
    final_data = filled_brand.filter(F.col("brand") != "na")

    #missing_cnts = final_data.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in final_data.columns])

    # show data
    final_data.show()


if __name__ == '__main__':
    spark = SparkSession.builder.appName('transactions etl').getOrCreate()
    assert spark.version >= '3.0'  # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main()
