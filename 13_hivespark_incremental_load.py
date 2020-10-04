from pyspark.sql import SparkSession
spark = SparkSession \
        .builder \
        .appName("Python Spark Incremental") \
        .config("spark.sql.uris", "thrift://hive-metastore:9083") \
        .config("hive.exec.dynamic.partition", "true") \
        .config("hive.exec.dynamic.partition.mode", "nonstrict") \
        .enableHiveSupport() \
        .getOrCreate()

spark.sql('DROP TABLE IF EXISTS employee_base')

'''Below create statement is not used'''
'''spark.sql("CREATE TABLE employee_base ( \
           id            INT,      \
           name          STRING,   \
           gender        STRING,   \ 
           designation   STRING,   \
           phone         STRING,   \
           email         STRING,   \
           modified      STRING    \
          )")'''


employee_base_df = spark.read.option("header", "true").csv('/user/employee.csv')

employee_base_df.write.format("csv").option("header", "true").mode("append").saveAsTable("employee_base")

spark.sql("select * from employee_base").show(3)

spark.sql('DROP TABLE IF EXISTS employee_incremental')

employee_incremental_df = spark.read.option("header", "true").csv('/user/employee_incremental.csv')

employee_incremental_df.write.format("csv").option("header", "true").mode("overwrite").saveAsTable("employee_incremental")

spark.sql("select * from employee_incremental").show(3)

spark.sql('DROP VIEW IF EXISTS reconcile_view')

spark.sql("CREATE VIEW reconcile_view AS \
SELECT t2.id,t2.name,t2.gender,t2.designation,t2.phone,t2.email,t2.modifiedon FROM \
(SELECT *,ROW_NUMBER() OVER (PARTITION BY id ORDER BY modifiedon DESC) rn \
FROM (SELECT * FROM employee_base \
UNION ALL \
SELECT * FROM employee_incremental) t1) t2 \
WHERE rn = 1")

spark.sql('DROP TABLE IF EXISTS reporting_table')

spark.sql("CREATE TABLE reporting_table AS SELECT * FROM reconcile_view")

spark.sql("SELECT * FROM reporting_table").show()


