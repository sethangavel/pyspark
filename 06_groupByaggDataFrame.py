from pyspark.sql.session import SparkSession
from pyspark.sql.functions import avg,format_number
spark = SparkSession. \
  builder. \
  appName('DF Aggregation Example'). \
  getOrCreate()

df_nycflights = spark.read. \
      csv('/user/nycflights13.csv',inferSchema=True,header=True)

'''monthly count'''
df_nycflights_monthly_count = df_nycflights. \
                              groupby('month'). \
                              count()

df_nycflights_monthly_count.show()

'''Finding average dep_delay and average arrival delay for each month'''
df_nycflights_agg = df_nycflights. \
                    groupby('month'). \
                    agg({'dep_delay': 'avg' , 'arr_delay': 'avg'})

df_nycflights_agg.show()

'''Finding miniumum and maximum dep_delay for each month'''
df_nycflights_max_min_dep_delay= df_nycflights. \
                                 groupby('month'). \
                                 agg({'dep_delay': 'min', 'dep_delay': 'max'})

df_nycflights_max_min_dep_delay.show()

'''Finding groupby with multiple columns'''
df_nycflights_grpby = df_nycflights. \
                      groupby(['month','origin','dest']). \
                      count(). \
                      orderBy(['month','count'],
                              ascending= [1,0])

df_nycflights_grpby.show()

'''group by also renaming column from group(column_name) to something proper '''
'''then using withColumn we do rounding as well'''
df_nycflights_agg_col_rename = df_nycflights. \
                               groupby('month'). \
                               agg({'dep_delay': 'avg' , 'arr_delay': 'avg'}). \
                               withColumnRenamed('avg(arr_delay)','mean_arr_delay'). \
                               withColumnRenamed('avg(dep_delay)','mean_dep_delay'). \
                               withColumn('mean_arr_delay',format_number('mean_arr_delay',1))


df_nycflights_agg_col_rename.show()

'''Group By with pivot'''
df_pivot_carrier = df_nycflights. \
                   groupby('month'). \
                   pivot('carrier'). \
                   count()

df_pivot_carrier.show()                       




