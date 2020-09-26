from pyspark.sql.functions import concat_ws,to_utc_timestamp 
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import avg,format_number,year,month,hour,minute,dayofmonth,date_sub,date_add,datediff,months_between
spark = SparkSession. \
  builder. \
  appName('DF DateTime Example'). \
  getOrCreate()

df_nycflights = spark.read. \
      csv('/user/nycflights13.csv',inferSchema=True,header=True)

'''Concatenating year month and day then hour and minute to make a field date'''
'''We are trying to create timestamp field'''
df_nycflights = df_nycflights. \
                withColumn('date',
                            concat_ws('-',
                                      df_nycflights.year,
                                      df_nycflights.month,
                                      df_nycflights.day)). \
                withColumn('time',
                            concat_ws(':',
                                      df_nycflights.hour,
                                      df_nycflights.minute)) 

df_nycflights.show()

df_nycflights = df_nycflights. \
                withColumn('timestamp',
                            concat_ws(' ',
                                      df_nycflights.date,
                                      df_nycflights.time)) 

df_nycflights.show()

'''Next we will convert the StringType to TimeStampType'''
df_nycflights = df_nycflights. \
                withColumn('timestamp',
                            to_utc_timestamp(df_nycflights.timestamp,'GMT'))

df_nycflights.show()

'''Next we make the column timestamp as first column'''
'''Note columns will return all column name'''
df_nycflights = df_nycflights. \
                select(df_nycflights.columns[-1:] + df_nycflights.columns[0:-1])

df_nycflights.show()


'''Now we drop year,month,day,hour,minute,date,time columns as we will again try to create these from timestamp column that we created'''
df_nycflights = df_nycflights. \
                drop('year'). \
                drop('month'). \
                drop('day'). \
                drop('hour'). \
                drop('minute'). \
                drop('date'). \
                drop('time')

df_nycflights.show() 

'''Now we extract the fields back'''
df_nycflights = df_nycflights. \
                withColumn('year',year(df_nycflights.timestamp)). \
                withColumn('month',month(df_nycflights.timestamp)). \
                withColumn('day',dayofmonth(df_nycflights.timestamp)). \
                withColumn('hour',hour(df_nycflights.timestamp)). \
                withColumn('minute',minute(df_nycflights.timestamp))  

df_nycflights.show()

'''Now few operations on timestamp '''
df_nycflights = df_nycflights.\
                withColumn('date_sub',date_sub(df_nycflights.timestamp ,10)). \
                withColumn('date_add',date_add(df_nycflights.timestamp ,10)). \
                withColumn('months_between',months_between(df_nycflights.timestamp,df_nycflights.timestamp))

df_nycflights.show()                    

