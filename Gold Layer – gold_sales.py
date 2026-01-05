from pyspark.sql.functions import sum

gold_df = spark.read.parquet(
    "abfss://silver@datalake.dfs.core.windows.net/sales/"
)

agg_df = gold_df.groupBy("Region", "Year") \
    .agg(sum("SalesAmount").alias("TotalSales"))

agg_df.write.mode("overwrite") \
    .format("parquet") \
    .save("abfss://gold@datalake.dfs.core.windows.net/sales/")
