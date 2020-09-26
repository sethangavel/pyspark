from pyspark.sql.session import SparkSession
from pyspark.sql.types import *

spark = SparkSession. \
  builder. \
  appName('Join df'). \
  getOrCreate()

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
      csv('/user/weather.csv',schema=weather_schema,header=True)

df_weather.show()

df_nycflights = spark.read. \
      csv('/user/nycflights13.csv',inferSchema=True,header=True)

df_nycflights.show()

'''Now we do left-join'''
df_nycflights_weather = df_nycflights. \
                        join (df_weather,
                              [df_nycflights.month == df_weather.month,
                               df_nycflights.day   == df_weather.day,
                               df_nycflights.hour == df_weather.hour],
                               'left_outer')

df_nycflights_weather.show()

beaufort_land_schema = StructType([  
  StructField('force', IntegerType(), True),
  StructField('speed_mi_h_lb', IntegerType(), True),
  StructField('speed_mi_h_ub', IntegerType(), True),
  StructField('name', StringType(), True)
  ])

df_beaufort_land = spark.read. \
                   csv('/user/beaufort_land.csv',schema=beaufort_land_schema,header=True)

df_beaufort_land.show()


'''Now we reduce columns using select'''
df_nycflights_wind_visib = df_nycflights_weather . \
                           select([ 'carrier', 'flight','origin', 'dest', 'wind_dir','wind_speed', 'wind_gust', 'visib'])


df_nycflights_wind_visib.show()


nycflights_wind_visib_beaufort = df_nycflights_weather . \
                                 join (df_beaufort_land,
                                       [df_nycflights_wind_visib.wind_speed >= df_beaufort_land.speed_mi_h_lb,
                                        df_nycflights_wind_visib.wind_speed <   df_beaufort_land.speed_mi_h_ub],
                                        'left_outer') . \
                                 withColumn('month',month(df_nycflights_wind_visib.timestamp)). \
                                 drop('speed_mi_h_lb') .\
                                 drop('speed_mi_h_ub')   

nycflights_wind_visib_beaufort.show()
