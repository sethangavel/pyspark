import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import flatten
from pyspark.sql.functions import arrays_zip, col
from functools import reduce
from pyspark.sql import DataFrame
spark = SparkSession.builder.appName('nested array type').getOrCreate()

arrayArrayData = [
  ("James",[["Java","Scala","C++"],["Spark","Java"]]),
  ("Michael",[["Spark","Java","C++"],["Spark","Java"]]),
  ("Robert",[["CSharp","VB"],["Spark","Python"]])
]

df = spark.createDataFrame(data=arrayArrayData, schema = ['name','subjects'])
df.printSchema()
df.show(truncate=False)

df_explode=df.select(df.name,explode(df.subjects).alias("Exploded_Subjects"))

df_explode.printSchema()
df_explode.show(truncate=False)

df_flatten=df.select(df.name,flatten(df.subjects).alias("Flattened_Subjects"))

df_flatten.printSchema()
df_flatten.show(truncate=False)

df_flatten_zip=df_flatten \
               .withColumn("tmp", arrays_zip("Flattened_Subjects"))  \
               .withColumn("tmp", explode("tmp")) \
               .select("name", col("tmp.Flattened_Subjects")) \

df_flatten_zip.printSchema()
df_flatten_zip.show(truncate=False)

'''Above is not performant hence below solution if array size is known '''
# Length of array
n = 5

# For legacy Python you'll need a separate function
# in place of method accessor
reduce(
    DataFrame.unionAll,
    (df_flatten.select("name", col("Flattened_Subjects").getItem(i))
        for i in range(n))
).toDF("name", "Subjects").show()