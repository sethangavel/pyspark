from pyspark.sql.session import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import col
from pyspark.sql.functions import flatten
from pyspark.sql.functions import array_contains
from pyspark.sql.functions import arrays_zip, col
spark = SparkSession. \
  builder. \
  appName('Column Operation Example'). \
  getOrCreate()


df = spark.read.format("json").load("/home/bose/PycharmProjects/pyspark3/datasources/2015-summary.json")
df.createOrReplaceTempView("dfTable")

'''Python way'''
df.select("DEST_COUNTRY_NAME").show(2)

'''SQL Way'''
dfsql=spark.sql("SELECT DEST_COUNTRY_NAME FROM dfTable LIMIT 2")
dfsql.show()

'''Python Way'''
df.select("DEST_COUNTRY_NAME", "ORIGIN_COUNTRY_NAME").show(2)

'''We use col and column both as they are same'''
from pyspark.sql.functions import expr, col, column
df.select(
expr("DEST_COUNTRY_NAME"),
col("DEST_COUNTRY_NAME"),
column("DEST_COUNTRY_NAME"))\
.show(2)

'''col and normal column'''
df.select(col("DEST_COUNTRY_NAME"), "DEST_COUNTRY_NAME").show(2)

'''expr with AS keyword'''
df.select(expr("DEST_COUNTRY_NAME AS destination")).show(2)

'''again rename with expr and .alias'''
df.select(expr("DEST_COUNTRY_NAME as destination").alias("DEST_COUNTRY_NAME"))\
.show(2)


'''selectExpr with condition check-Python'''
df.selectExpr(
"*", # all original columns
"(DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME) as withinCountry")\
.show(2)

'''Same as above with SQL'''
spark.sql("SELECT *, (DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME) as withinCountry FROM dfTable LIMIT 2").show()

'''Aggregate avg and count'''
df.selectExpr("avg(count)", "count(distinct(DEST_COUNTRY_NAME))").show(2)

'''Aggregation sql'''
spark.sql("SELECT avg(count), count(distinct(DEST_COUNTRY_NAME)) FROM dfTable LIMIT 2").show()

'''literal i.e constant column'''
from pyspark.sql.functions import lit
df.select(expr("*"), lit(1).alias("One")).show(2)

'''withColumn -adding column,one is column and second is expression'''
df.withColumn("withinCountry", expr("ORIGIN_COUNTRY_NAME == DEST_COUNTRY_NAME"))\
.show(2)

'''renaming column- withColumnRenamed'''
print(df.withColumnRenamed("DEST_COUNTRY_NAME", "dest").columns)


'''We will try long column name'''
dfWithLongColName = df.withColumn(
"This Long Column-Name",
expr("ORIGIN_COUNTRY_NAME"))

'''Below without backtick long column name fails as here we are referencing long column'''
dfWithLongColName.selectExpr(
"`This Long Column-Name`",
"`This Long Column-Name` as `new col`")\
.show(2)


'''We are referring Long keyword but with backtick no error removing same fails'''
print(dfWithLongColName.select(expr("`This Long Column-Name`")).columns)

'''Spark is case insensitive,incase you want to make it case sensitive use below'''
'''set spark.sql.caseSensitive true'''

'''Removing a column'''
print(df.drop("ORIGIN_COUNTRY_NAME").columns)

'''casting (conversion to other type)'''
df.withColumn("count2", col("count").cast("long"))

df.printSchema()


'''Filter in spark'''
df.filter(col("count") < 2).show(2)
df.where("count < 2").show(2)

'''where clause'''
df.where(col("count") < 2).where(col("ORIGIN_COUNTRY_NAME") != "Croatia")\
.show(2)

'''Getting unique row'''
df.select("ORIGIN_COUNTRY_NAME", "DEST_COUNTRY_NAME").distinct().count()


from pyspark.sql import Row
schema = df.schema
newRows = [
Row("New Country", "Other Country", 5),
Row("New Country 2", "Other Country 3", 1)
]
parallelizedRows = spark.sparkContext.parallelize(newRows)
newDF = spark.createDataFrame(parallelizedRows, schema)

'''Union between newRows and df'''
df.union(newDF)\
.where("count = 1")\
.where(col("ORIGIN_COUNTRY_NAME") != "United States")\
.show()

'''sort by column count'''
df.sort("count").show(5)
'''orderBy count and DEST_COUNTRY_NAME'''
df.orderBy("count", "DEST_COUNTRY_NAME").show(5)
df.orderBy(col("count"), col("DEST_COUNTRY_NAME")).show(5)

'''Limit '''
df.limit(5).show()