from pyspark.sql.session import SparkSession
from pyspark.sql import Row
import pyspark.sql.functions as f
spark = SparkSession. \
  builder. \
  appName('MapType Example'). \
  getOrCreate()

data = [({'a': 1, 'b': 2},), ({'c':3},), ({'a': 4, 'c': 5},)]
df_map = spark.createDataFrame(data, ["a"])

df_map.show()

df_map.printSchema()

''' Flattening map'''
df_new = df_map.select(
    f.struct(*[f.col("a").getItem(c).alias(c) for c in ["a", "b", "c"]]).alias("a")
)
df_new.show()

'''Flattening maptype to rows'''
df_map.withColumn("id", f.monotonically_increasing_id())\
    .select("id", f.explode("a"))\
    .groupby("id")\
    .pivot("key")\
    .agg(f.first("value"))\
    .drop("id")\
    .show()