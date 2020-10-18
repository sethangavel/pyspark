from pyspark.sql.session import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import col
from pyspark.sql.functions import flatten
from pyspark.sql.functions import array_contains
from pyspark.sql.functions import arrays_zip, col
spark = SparkSession. \
  builder. \
  appName('ArrayType Example'). \
  getOrCreate()

df_array = spark.createDataFrame([ \
    Row(arrayA=[1,2,3,4,5],fieldB="Rory"),Row(arrayA=[888,777,555,999,666],fieldB="Arin")])

df_array.show()

''' Finding in arraytype'''
df_finding_in_array = df_array.filter(array_contains(df_array.arrayA,3))
df_finding_in_array.show()


''' Getting an element from an ArrayType '''
df_array.select( \
        col("arrayA").getItem(0).alias("element0"), \
        col("arrayA")[4].alias("element5"), \
        col("fieldB")) \
    .show()

''' StructType nested in ArrayType '''
df_nested_arraytype = spark.createDataFrame([
    Row(
        arrayA=[
            Row(childStructB=Row(field1=1, field2="foo")),
            Row(childStructB=Row(field1=2, field2="bar"))
        ]
    )])
df_nested_arraytype.printSchema()

df_nested_arraytype.show(1, False)


'''Printing arrayA, field1 and field2 using dot '''
df_child = df_nested_arraytype.select(
        "arrayA.childStructB.field1",
        "arrayA.childStructB.field2")

df_child.printSchema()

df_child.show()


'''Nested structype within nested arraytype'''
df_nested_B = spark.createDataFrame([
    Row(
        arrayA=[[
            Row(childStructB=Row(field1=1, field2="foo")),
            Row(childStructB=Row(field1=2, field2="bar"))
        ]]
    )])
df_nested_B.printSchema()



