%python

airquality = spark.read.format('csv').options(header='true', inferSchema='true').load('/FileStore/tables/airquality.csv')

airquality.show()



%python 
    import pandas as pd
    def write_parquet_file():
        df = spark.read.format('csv').options(header='true', inferSchema='true').load('/FileStore/tables/airquality.csv')
        df.write.mode("overwrite").parquet('/FileStore/tables/airquality.parquet')
    write_parquet_file()


'''https://www.ellicium.com/parquet-file-format-structure/'''
'''While querying columnar storage, it skips the nonrelevant data very quickly, making faster query execution. 
As a result aggregation queries consume less time compared to row-oriented databases.'''
parqDF = spark.read.option("header","true").parquet("/FileStore/tables/airquality.parquet")
parqDF.show(10)



'''Using SQL with parquet'''
parqDF.createOrReplaceTempView("airqualityparquettbl")
parkSQL = spark.sql("select * from airqualityparquettbl where Temp >= 60 ")
display(parkSQL)




'''Creating templ table directly on file'''
spark.sql("CREATE TEMPORARY VIEW AIRQUALITY USING parquet OPTIONS (path \"/FileStore/tables/airquality.parquet\")")
spark.sql("SELECT * FROM AIRQUALITY").show()


'''Next we will see partition on parquet'''
data =[("James ","","Smith","36636","M",3000),
              ("Michael ","Rose","","40288","M",4000),
              ("Robert ","","Williams","42114","M",4000),
              ("Maria ","Anne","Jones","39192","F",4000),
              ("Jen","Mary","Brown","","F",-1)]
columns=["firstname","middlename","lastname","dob","gender","salary"]
df=spark.createDataFrame(data,columns)
df.show()



'''partitionBy salary and gender'''
df.write.mode("overwrite").partitionBy("gender","salary").parquet("/FileStore/tables/people2.parquet")
parqDF = spark.read.parquet("/FileStore/tables/people2.parquet")
parqDF.createOrReplaceTempView("Table2")
df = spark.sql("select * from Table2  where gender='M' and salary >= 4000")
df.show(3)


parqMDF = spark.read .parquet("/FileStore/tables/people2.parquet/gender=M")
display(parqMDF)



