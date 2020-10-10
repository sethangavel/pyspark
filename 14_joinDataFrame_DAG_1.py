from pyspark.sql.session import SparkSession
from pyspark.sql.types import *
import threading

spark = SparkSession. \
  builder. \
  appName('Join DAG df'). \
  getOrCreate()

sc = spark.sparkContext

def timer_elapsed():
    print('Timer elapsed')
    if not sc._jsc.sc().isStopped():
      spark.stop()

weather_schema = StructType([  
  StructField('year', IntegerType(), True),
  StructField('month', IntegerType(), True),
  StructField('day', IntegerType(), True),
  StructField('hour', IntegerType(), True),
  StructField('temp', FloatType(), True),
  StructField('dewp', FloatType(), True),
  StructField('humid', FloatType(), True),
  StructField('wind_dir', IntegerType(), True),
  StructField('wind_speed', FloatType(), True),
  StructField('wind_gust', FloatType(), True),
  StructField('precip', FloatType(), True),
  StructField('pressure', FloatType(), True),
  StructField('visib', FloatType(), True)
  ])

df_weather = spark.read. \
      csv('/user/weather_noheader.csv',header=False)

df_weather.show()

df_nycflights = spark.read. \
      csv('/user/nycflights13_noheader.csv',header=False)

df_nycflights.show()

'''Now we do left-join'''
'''df_nycflights_weather = df_nycflights. \
                        join (df_weather,
                              [df_nycflights.month == df_weather.month,
                               df_nycflights.day   == df_weather.day,
                               df_nycflights.hour == df_weather.hour],
                               'left_outer')'''

'''df_nycflights_weather.show()'''

''' wait for 300 sec for Spark job to complete'''
spark_timer = threading.Timer(300, timer_elapsed)
spark_timer.start()
