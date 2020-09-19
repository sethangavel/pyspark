from pyspark.sql.session import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from array import array
from pyspark.sql.types import *
spark = SparkSession. \
  builder. \
  appName('CSV Example custom schema'). \
  getOrCreate()

schema = StructType([
    StructField("CustomerName", StringType(), True),
    StructField("PersonName", StringType(), True),
    StructField("AmountPaid", StringType(), True)])

spark.read.csv("/user/sales_info.csv", header=False, schema=schema).show()

