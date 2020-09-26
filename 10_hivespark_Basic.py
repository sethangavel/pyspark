from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, HiveContext
SparkContext.setSystemProperty("hive.metastore.uris", "thrift://hive-metastore:9083")
sparkSession = (SparkSession
                .builder
                .appName('example-pyspark-read-and-write-from-hive')
                .config("hive.metastore.uris", "thrift://hive-metastore:9083", conf=SparkConf())
                .enableHiveSupport()
                .getOrCreate()
                )
data = [('First', 1), ('Second', 2), ('Third', 3), ('Fourth', 4), ('Fifth', 5)]
df = sparkSession.createDataFrame(data)
df.write.saveAsTable('example')

