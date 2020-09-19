from pyspark.sql.session import SparkSession
spark = SparkSession. \
  builder. \
  appName('CSV Example'). \
  getOrCreate()

df2 = spark.read. \
      csv('/user/sales_info.csv',inferSchema=True,header=True)
df2.show()


