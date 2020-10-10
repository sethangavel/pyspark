from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.rdd import portable_hash
from pyspark import Row

appName = "PySpark Partition Example"
master = "local[8]"

# Create Spark session with Hive supported.
spark = SparkSession.builder \
    .appName(appName) \
    .master(master) \
    .getOrCreate()
print(spark.version)
spark.sparkContext.setLogLevel("ERROR")

# Populate sample data
countries = ("CN", "AU", "US")
data = []
for i in range(1, 13):
    data.append({"ID": i, "Country": countries[i % 3],  "Amount": 10+i})

def print_partitions(df):
    numPartitions = df.rdd.getNumPartitions()
    print("Total partitions: {}".format(numPartitions))
    print("Partitioner: {}".format(df.rdd.partitioner))
    df.explain()
    parts = df.rdd.glom().collect()
    i = 0
    j = 0
    for p in parts:
        print("Partition {}:".format(i))
        for r in p:
            print("Row {}:{}".format(j, r))
            j = j+1
        i = i+1


df = spark.createDataFrame(data)
df.show()
print_partitions(df)

'''Repartition data to 3 partition by Country'''
'''We expect each country data to be in each partition but thats not possible as partitoning is done based on hash partitioning
   so different country code might fall in same partition'''
numPartitions = 3

df = df.repartition(numPartitions, "Country")

print_partitions(df)

'''We can verify the hash value of each partition based on which it repartitions '''
udf_portable_hash = udf(lambda str: portable_hash(str))
df = df.withColumn("Hash#", udf_portable_hash(df.Country))
df = df.withColumn("Partition#", df["Hash#"] % numPartitions)
df.show()

'''Increasing partition to 5 puts data for each country in  different partition but thats not confirmed in all scenario'''
numPartitions = 5
df = df.repartition(numPartitions, "Country")
print_partitions(df)
udf_portable_hash = udf(lambda str: portable_hash(str))
df = df.withColumn("Hash#", udf_portable_hash(df.Country))
df = df.withColumn("Partition#", df["Hash#"] % numPartitions)
df.show()

'''To address this issue we use customised partition'''
'''udf below'''
def country_partitioning(k):
    return countries.index(k)

'''registering udf '''    
udf_country_hash = udf(lambda str: country_partitioning(str))

'''mentioning partitionBy with udf i.e customized partitioning function'''
df = df.rdd \
    .map(lambda el: (el["Country"], el)) \
    .partitionBy(numPartitions, country_partitioning) \
    .toDF()
print_partitions(df)

df = df.withColumn("Hash#", udf_country_hash(df[0]))
'''Below is hash divided by number of partitions '''
df = df.withColumn("Partition#", df["Hash#"] % numPartitions)
df.show()
