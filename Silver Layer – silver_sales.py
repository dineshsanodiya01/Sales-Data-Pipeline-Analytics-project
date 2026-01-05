from pyspark.sql.functions import col

silver_df = spark.read.parquet(
    "abfss://bronze@datalake.dfs.core.windows.net/sales/"
)

silver_df = silver_df.dropDuplicates() \
    .filter(col("SalesAmount").isNotNull())

silver_df.write.mode("overwrite") \
    .format("parquet") \
    .save("abfss://silver@datalake.dfs.core.windows.net/sales/")
