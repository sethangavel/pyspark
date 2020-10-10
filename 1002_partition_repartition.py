from pyspark.sql.functions import year, month, dayofmonth
from pyspark.sql import SparkSession
from datetime import date, timedelta
from pyspark.sql.types import IntegerType, DateType, StringType, StructType, StructField

appName = "PySpark Partition Example"


# Create Spark session with Hive supported.
spark = SparkSession.builder \
    .appName(appName) \
    .getOrCreate()

print(spark.version)
# Populate sample data
start_date = date(2019, 1, 1)
data = []
for i in range(0, 50):
    data.append({"Country": "CN", "Date": start_date +
                 timedelta(days=i), "Amount": 10+i})
    data.append({"Country": "AU", "Date": start_date +
                 timedelta(days=i), "Amount": 10+i})

schema = StructType([StructField('Country', StringType(), nullable=False),
                     StructField('Date', DateType(), nullable=False),
                     StructField('Amount', IntegerType(), nullable=False)])

df = spark.createDataFrame(data, schema=schema)
df.show()

print(df.rdd.getNumPartitions())

df.write.mode("overwrite").csv("/user/example0.csv", header=True)


'''coalesce if higher is mentioned then it wont happen as coalesce doesnt shuffle data'''
df = df.coalesce(16)
'''Below count remains same as initial if the partition count initially was less than 16 '''
print(df.rdd.getNumPartitions())
df.write.mode("overwrite").csv("data/example16.csv", header=True)


'''repartition will change partition count to 10'''
df = df.repartition(10)
print(df.rdd.getNumPartitions())
df.write.mode("overwrite").csv("/user/example1.csv", header=True)


'''repartition based on column'''
df = df.repartition("Country")
'''By default 200 partitions are created, many partition having no data ,controlled by spark.sql.shuffle.partitions '''
print(df.rdd.getNumPartitions())
df.write.mode("overwrite").csv("/user/example2.csv", header=True)


'''Next we apply multiple partition '''
df = df.withColumn("Year", year("Date")).withColumn("Month", month("Date")).withColumn("Day", dayofmonth("Date"))
df = df.repartition("Year", "Month", "Day", "Country")
print(df.rdd.getNumPartitions())
df.write.mode("overwrite").csv("/user/example3.csv", header=True)

'''Match repartition keys with write partition keys'''
df.write.partitionBy("Year", "Month", "Day", "Country").mode("overwrite").csv("/user/example22.csv", header=True)

'''Reading Month=2 and Day=1 for Country=CN'''
df = spark.read.csv("/user/example22.csv/Year=2019/Month=2/Day=1/Country=CN")
print(df.rdd.getNumPartitions())
df.show()

'''Reading Month=2 data for Year=2019'''
df = spark.read.csv("/user/example22.csv/Year=2019/Month=2")
print(df.rdd.getNumPartitions())
df.show()

'''wildcards for partition discovery here "" is base path and here we read Country=CN data for all days and month '''
df = spark.read.option("", "/user/example22.csv/").csv(
"/user/example22.csv/Year=*/Month=*/Day=*/Country=CN")
print(df.rdd.getNumPartitions())
df.show()

'''wildcard can be used at any part for wildcard partition discovery'''
df = spark.read.option("", "/user/example22.csv/").csv(
"/user/example22.csv/Year=*/Month=2/Day=*/Country=AU")
print(df.rdd.getNumPartitions())
df.show()
