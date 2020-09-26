from pyspark.sql.session import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import udf, log
from pyspark.sql.functions import avg,format_number
spark = SparkSession. \
  builder. \
  appName('UDF df'). \
  getOrCreate()

df_beaufort_land = spark.read. \
      csv('/user/beaufort_land.csv',inferSchema=True,header=True)

df_beaufort_land.show()

'''Simple udf ,if we multiple with 2 not 2.0 then null will be returned'''
def mph_to_mps(myinput):
  myoutput = myinput * 2.0
  return myoutput

'''Registering udf so that we can define return type'''
mph_to_mps = udf(mph_to_mps, FloatType())

df_beaufort_land = df_beaufort_land. \
                   withColumn('udf_fn_col',mph_to_mps('speed_mi_h_lb'))

df_beaufort_land.show()




