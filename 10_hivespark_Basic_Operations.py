from pyspark.sql import SparkSession
spark = SparkSession \
        .builder \
        .appName("Python Spark SQL Hive integration example") \
        .config("spark.sql.uris", "thrift://hive-metastore:9083") \
        .enableHiveSupport() \
        .getOrCreate()
# Read from HDFS
df_load = spark.read.csv('/user/movies.csv')
df_load.show()

spark.sql('DROP TABLE IF EXISTS movies');
spark.sql('DROP TABLE IF EXISTS movies_orc');
spark.sql('DROP TABLE IF EXISTS movies_parquet');
spark.sql('DROP TABLE IF EXISTS movies_avro');


spark.sql("CREATE TABLE movies ( \
movieid INT, \
title   STRING, \
genre   STRING \
)  STORED AS TEXTFILE")

spark.sql("CREATE TABLE movies_orc ( \
movieid INT, \
title   STRING, \
genre   STRING \
)  STORED AS ORC")

spark.sql("CREATE TABLE movies_parquet ( \
movieid INT, \
title   STRING, \
genre   STRING \
)  STORED AS PARQUET")

spark.sql("CREATE TABLE movies_avro ( \
movieid INT, \
title   STRING, \
genre   STRING \
)  STORED AS AVRO")


df_load.write.mode('overwrite').insertInto('movies')

df_load.write.mode('overwrite').insertInto('movies_orc')

df_load.write.mode('overwrite').insertInto('movies_parquet')

df_load.write.mode('overwrite').insertInto('movies_avro')

