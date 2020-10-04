from pyspark.sql import SparkSession
spark = SparkSession \
        .builder \
        .appName("Python Spark SQL Json Load Into Partition") \
        .config("spark.sql.uris", "thrift://hive-metastore:9083") \
        .config("hive.exec.dynamic.partition", "true") \
        .config("hive.exec.dynamic.partition.mode", "nonstrict") \
        .enableHiveSupport() \
        .getOrCreate()

'''Loading json file also the sequence of column has to be proper ,as partition is country hence for insertinto it need to be last column'''
jsonDF_load = spark.read.json('/user/jsondata.txt') \
              .select("designation", "email", "first_name", "gender", "id", "last_name","phone","country")

jsonDF_load.printSchema()

jsonDF_load.show(3)

spark.sql('DROP TABLE IF EXISTS cust_partition')


'''dynamic partition mode = nonstrict and dynamic partition need to be true'''
'''dynamic partition means that partition not needed to be defined manually they are created based on data'''
spark.sql("CREATE TABLE cust_partition( \
     designation STRING, \
     email STRING, \
     first_name STRING, \
     gender STRING, \
     id INT, \
     last_name STRING, \
     phone STRING )  \
    PARTITIONED BY \
    (country STRING) \
    STORED AS ORC \
    TBLPROPERTIES \
    ('hive.exec.dynamic.partition.mode'='nonstrict', \
     'transactional'='true', \
    'hive.exec.dynamic.partition'='true', \
    'orc.compress'='SNAPPY')")

'''insertInto inserts data into cust_partition and also creates hdfs folder, we can do append as well as overwrite'''
jsonDF_load.write.mode("append").insertInto("cust_partition")

spark.sql("SELECT * FROM cust_partition LIMIT 3").show()

'''This shows all partitions of the table'''
spark.sql("SHOW PARTITIONS cust_partition").show()

spark.sql("ALTER TABLE cust_partition ADD COLUMNS (access_count1 int)")

'''Once you do this check in  hdfs or even check partitions they will be no entry as data still Benin but name partition now on Togo and Benin ''' 

'''Rename partition'''
spark.sql("ALTER TABLE cust_partition partition(country='Benin') rename to partition(country='Togo and Benin')")


'''This shows all partitions of the table again'''
spark.sql("SHOW PARTITIONS cust_partition").show()

