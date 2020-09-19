from pyspark.sql.session import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from array import array
from pyspark.sql.types import *
from pyspark.sql.functions import col
spark = SparkSession. \
  builder. \
  appName('CSV Example custom schema withColumn'). \
  getOrCreate()
CONST = 10

schema = StructType([
    StructField("CustomerName", StringType(), True),
    StructField("PersonName", StringType(), True),
    StructField("AmountPaid", StringType(), True)])

df=spark.read.csv("/user/sales_info.csv", header=True, schema=schema)

df.show()

'''Checking datatype its dataframe or rdd'''
print(type(df))


'''Modifying column value for Amount Paid'''
df=df.withColumn("AmountPaid",col("AmountPaid").cast("Integer"))

'''Checking schema of dataframe '''
print(df.schema)

df=df.withColumn("AmountPaid",col("AmountPaid")*CONST)


df.show()
