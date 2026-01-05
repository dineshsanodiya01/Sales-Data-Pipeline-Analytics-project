from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("BronzeSales").getOrCreate()

bronze_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("abfss://raw@datalake.dfs.core.windows.net/sales/")

bronze_df.write.mode("overwrite") \
    .format("parquet") \
    .save("abfss://bronze@datalake.dfs.core.windows.net/sales/")
