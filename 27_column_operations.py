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
dfsql = spark.sql("SELECT DEST_COUNTRY_NAME FROM dfTable LIMIT 2")
dfsql.show()

'''Python Way'''
df.select("DEST_COUNTRY_NAME", "ORIGIN_COUNTRY_NAME").show(2)

'''We use col and column both as they are same'''
from pyspark.sql.functions import expr, col, column

df.select(
    expr("DEST_COUNTRY_NAME"),
    col("DEST_COUNTRY_NAME"),
    column("DEST_COUNTRY_NAME")) \
    .show(2)

'''col and normal column'''
df.select(col("DEST_COUNTRY_NAME"), "DEST_COUNTRY_NAME").show(2)

'''expr with AS keyword'''
df.select(expr("DEST_COUNTRY_NAME AS destination")).show(2)

'''again rename with expr and .alias'''
df.select(expr("DEST_COUNTRY_NAME as destination").alias("DEST_COUNTRY_NAME")) \
    .show(2)

'''selectExpr with condition check-Python'''
df.selectExpr(
    "*",  # all original columns
    "(DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME) as withinCountry") \
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
df.withColumn("withinCountry", expr("ORIGIN_COUNTRY_NAME == DEST_COUNTRY_NAME")) \
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
    "`This Long Column-Name` as `new col`") \
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
df.where(col("count") < 2).where(col("ORIGIN_COUNTRY_NAME") != "Croatia") \
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
df.union(newDF) \
    .where("count = 1") \
    .where(col("ORIGIN_COUNTRY_NAME") != "United States") \
    .show()

'''sort by column count'''
df.sort("count").show(5)
'''orderBy count and DEST_COUNTRY_NAME'''
df.orderBy("count", "DEST_COUNTRY_NAME").show(5)
df.orderBy(col("count"), col("DEST_COUNTRY_NAME")).show(5)

'''Limit '''
df.limit(5).show()

df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/home/bose/PycharmProjects/pyspark3/datasources//2010-12-01.csv")
df.printSchema()
df.createOrReplaceTempView("dfTable")

# in Python- where with not equal
from pyspark.sql.functions import col

df.where(col("InvoiceNo") != 536365) \
    .select("InvoiceNo", "Description") \
    .show(5, False)

from pyspark.sql.functions import instr

'''filter condition defined priceFilter'''
priceFilter = col("UnitPrice") > 600
'''descripFilter filter defined '''
descripFilter = instr(df.Description, "POSTAGE") >= 1
'''below we apply the above conditions
   Stockcode is in DOT along with 2 filters'''
df.where(df.StockCode.isin("DOT")).where(priceFilter | descripFilter).show()

'''adding filter condition'''
from pyspark.sql.functions import instr

'''Below is DOTCodeFilter'''
DOTCodeFilter = col("StockCode") == "DOT"
'''Below is priceFilter'''
priceFilter = col("UnitPrice") > 600
'''Below is descripFilter'''
descripFilter = instr(col("Description"), "POSTAGE") >= 1
'''Now we apply the filters and create a new column isExpensive'''
df.withColumn("isExpensive", DOTCodeFilter & (priceFilter | descripFilter)) \
    .where("isExpensive") \
    .select("unitPrice", "isExpensive").show(5)

'''Working with numbers'''
from pyspark.sql.functions import expr, pow

'''we create a column from other columns Quantity and Unit Price'''
fabricatedQuantity = pow(col("Quantity") * col("UnitPrice"), 2) + 5
'''Next we print CustomerId and realQuantity on which fabricatedQuantity was applied'''
df.select(expr("CustomerId"), fabricatedQuantity.alias("realQuantity")).show(3)

'''same as above with selectExpr'''
df.selectExpr(
    "CustomerId",
    "(POWER((Quantity * UnitPrice), 2.0) + 5) as realQuantity").show(3)

'''To see data overview'''
df.describe().show()

'''Increasing row number incrementally'''
from pyspark.sql.functions import monotonically_increasing_id

df.select(monotonically_increasing_id()).show(3)

'''String Operations'''
from pyspark.sql.functions import initcap

'''initcap applied to description'''
df.select(initcap(col("Description"))).show(3)

from pyspark.sql.functions import lower, upper

'''upper and lower'''
df.select(col("Description"),
          lower(col("Description")),
          upper(lower(col("Description")))).show(2)

'''regexp_replace '''
from pyspark.sql.functions import regexp_replace

regex_string = "BLACK|WHITE|RED|GREEN|BLUE"
df.select(
    regexp_replace(col("Description"), regex_string, "COLOR").alias("color_clean"),
    col("Description")).show(2)

# translate used to replace single character with another
from pyspark.sql.functions import translate

df.select(translate(col("Description"), "LEET", "1337"), col("Description")) \
    .show(2)

'''regular expression '''
from pyspark.sql.functions import regexp_extract

extract_str = "(BLACK|WHITE|RED|GREEN|BLUE)"
df.select(
    regexp_extract(col("Description"), extract_str, 1).alias("color_clean"),
    col("Description")).show(2)

'''instring searching for a string'''
from pyspark.sql.functions import instr

containsBlack = instr(col("Description"), "BLACK") >= 1
containsWhite = instr(col("Description"), "WHITE") >= 1
'''new field hasSimpleColor which contains BLACK or WHITE'''
df.withColumn("hasSimpleColor", containsBlack | containsWhite) \
    .where("hasSimpleColor") \
    .select("Description").show(3, False)

from pyspark.sql.functions import expr, locate

simpleColors = ["black", "white", "red", "green", "blue"]
def color_locator(column, color_string):
    return locate(color_string.upper(), column) \
        .cast("boolean") \
        .alias("is_" + color_string)
selectedColumns = [color_locator(df.Description, c) for c in simpleColors]
selectedColumns.append(expr("*"))  # has to a be Column type
df.select(*selectedColumns).where(expr("is_white OR is_red")) \
    .select("Description").show(10, False)

'''------Timestamp Operations--------'''
from pyspark.sql.functions import current_date, current_timestamp
dateDF = spark.range(10)\
  .withColumn("today", current_date())\
  .withColumn("now", current_timestamp())
dateDF.createOrReplaceTempView("dateTable")
dateDF.printSchema()

'''Subtracting adding days to a date'''
from pyspark.sql.functions import date_add, date_sub
dateDF.select(date_sub(col("today"), 5), date_add(col("today"), 5)).show(1)

'''Finding diff of date add 7 days with todays create column and subtract todays date'''
from pyspark.sql.functions import datediff, months_between, to_date
dateDF.withColumn("week_ago", date_sub(col("today"), 7))\
  .select(datediff(col("week_ago"), col("today"))).show(1)

'''Month difference between days'''
dateDF.select(
   to_date(lit("2016-01-01")).alias("start"),
   to_date(lit("2017-05-22")).alias("end"))\
 .select(months_between(col("start"), col("end"))).show(1)

'''date in string literal format converted to date again'''
from pyspark.sql.functions import to_date, lit
spark.range(5).withColumn("date", lit("2017-01-01"))\
 .select(to_date(col("date"))).show(1)

'''Below it prints wrongly null and 11dec2017 whereas it was 12nov2017'''
dateDF.select(to_date(lit("2016-20-12")),to_date(lit("2017-12-11"))).show(1)

'''Next w provide the dateformat'''
from pyspark.sql.functions import to_date
dateFormat = "yyyy-dd-MM"
cleanDateDF = spark.range(1).select(
  to_date(lit("2017-12-11"), dateFormat).alias("date"),
  to_date(lit("2017-20-12"), dateFormat).alias("date2"))
cleanDateDF.createOrReplaceTempView("dateTable2")

'''now data printed properly'''
spark.sql("select * from dateTable2").show(1)

'''timestamp printing of date'''
from pyspark.sql.functions import to_timestamp
cleanDateDF.select(to_timestamp(col("date"), dateFormat)).show()

'''filter date using literal type'''
cleanDateDF.filter(col("date2") > lit("2017-12-12")).show()

'''coalesce - select the first non-null value from a set of columns'''
from pyspark.sql.functions import coalesce
df.select(coalesce(col("Description"), col("CustomerId"))).show(3)

'''all null is shown as- All Null values become this string                     '''
df.na.fill("All Null values become this string")

'''filling null in StockCode and InvoiceNo column with 5'''
df.na.fill("all", subset=["StockCode", "InvoiceNo"])

'''replacing column value "" with "UNKNOWN"'''
df.na.replace([""], ["UNKNOWN"], "Description")

'''explode and split'''
from pyspark.sql.functions import split, explode
df.withColumn("splitted", split(col("Description"), " "))\
 .withColumn("exploded", explode(col("splitted")))\
 .select("Description", "InvoiceNo", "exploded").show(2)

'''UDF Operation'''
'''Spark will serialize the function on the driver and transfer it over the network to all executor processes.'''
'''If the function is written in Python, something quite different happens. Spark starts a Python
process on the worker, serializes all of the data to a format that Python can understand
(remember, it was in the JVM earlier), executes the function row by row on that data in the
Python process, and then finally returns the results of the row operations to the JVM and Spark.'''
udfExampleDF = spark.range(5).toDF("num")
def power3(double_value):
  return double_value ** 3
print(power3(2.0))

