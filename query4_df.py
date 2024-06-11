from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, udf, avg, count, round 
from pyspark.sql.types import DoubleType
import geopy.distance

# Function to calculate distance between two points
def get_distance(lat1, lon1, lat2, lon2):
    return geopy.distance.geodesic((lat1, lon1), (lat2, lon2)).km

# Register UDF
get_distance_udf = udf(get_distance, DoubleType())

spark = SparkSession.builder.appName("query4_df").getOrCreate()

# Read the data
# crime_data = spark.read.csv("data/Crime_Data_from_2010_to_2019.csv", header=True, inferSchema=True)
# police_stations = spark.read.csv("data/LAPD_Police_Stations.csv", header=True, inferSchema=True)

crime_data1 = spark.read.csv("hdfs://master:9000/home/user/ergasia/Crime_Data_from_2010_to_2019.csv", header=True, inferSchema=True)
crime_data2 = spark.read.csv("hdfs://master:9000/home/user/ergasia/Crime_Data_from_2020_to_Present.csv", header=True, inferSchema=True)
crime_data = crime_data1.union(crime_data2) 

police_stations = spark.read.csv("hdfs://master:9000/home/user/ergasia/LAPD_Police_Stations.csv", header=True, inferSchema=True)

# Filter crime data for weapons starting with '1'
crime_data_guns = crime_data.filter(col('Weapon Used Cd').startswith('1'))
crime_data_guns = crime_data_guns.filter((crime_data_guns['LAT'] != '0') | (crime_data_guns['LON'] != '0'))

# Join the dataframes
joined_df = crime_data_guns.join(police_stations, crime_data_guns['AREA '] == police_stations["PREC"], "inner")

# Calculate distance and add as a new column
joined_df = joined_df.withColumn('distance', get_distance_udf(col('LAT'), col('LON'), col('Y'), col('X')))

# Group by division, count incidents, and calculate average distance
output_df = joined_df.groupby('division').agg(count('*').alias('incidents_total'),
    round(avg('distance'),4).alias('average_distance')
)

# Sort by incidents_total in descending order
output_df = output_df.orderBy(desc('incidents_total'))

# Show the result
output_df.show()

spark.stop() 
