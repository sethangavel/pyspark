'''Complex Types in Spark
   1.ArrayType- Array of Struct column
   2.MapType- KeyType and KeyValue, KeyType cannot be null
   3.StructType '''

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType

spark = SparkSession.builder.master("local[8]") \
                    .appName('SparkByExamples.com') \
                    .getOrCreate()

structureData = [
    (("James","","Smith"),"36636","M",3100),
    (("Michael","Rose",""),"40288","M",4300),
    (("Robert","","Williams"),"42114","M",1400),
    (("Maria","Anne","Jones"),"39192","F",5500),
    (("Jen","Mary","Brown"),"","F",-1)
  ]

structureSchema = StructType([
        StructField('name', StructType([
             StructField('firstname', StringType(), True),
             StructField('middlename', StringType(), True),
             StructField('lastname', StringType(), True)
             ])),
         StructField('id', StringType(), True),
         StructField('gender', StringType(), True),
         StructField('salary', IntegerType(), True)
         ])


df_struct = spark.createDataFrame(data=structureData,schema=structureSchema)
df_struct.printSchema()
df_struct.show(truncate=False)

df_struct.select("name.*", "id","gender","salary").printSchema()
df_struct_flatten=df_struct.select("name.*", "id","gender","salary")

df_struct_flatten.show(truncate=False)
