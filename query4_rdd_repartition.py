from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
import geopy.distance
from io import StringIO
import csv

sc = SparkSession.builder \
    .appName("query4_rdd_repartition") \
    .getOrCreate() \
    .sparkContext

# Function to calculate distance between two points
def get_distance(lat1, lon1, lat2, lon2):
    return geopy.distance.geodesic((lat1, lon1), (lat2, lon2)).km


def parse_csv(line):
    if not line.strip():
        return None
    sio = StringIO(line)
    reader = csv.reader(sio)
    return next(reader)

# Read the data
crime_data1 = sc.textFile("hdfs://master:9000/home/user/ergasia/Crime_Data_from_2010_to_2019.csv")
police_stations = sc.textFile("hdfs://master:9000/home/user/ergasia/LAPD_Police_Stations.csv")
crime_data2 = sc.textFile("hdfs://master:9000/home/user/ergasia/Crime_Data_from_2020_to_Present.csv")


# Parse the CSV files (assuming the first line is the header)
crime_header1 = crime_data1.first()
crime_data1 = crime_data1.filter(lambda row: row != crime_header1).map(parse_csv) \
                         .filter(lambda x: x is not None)
crime_header2 = crime_data2.first()
crime_data2 = crime_data2.filter(lambda row: row != crime_header2).map(parse_csv) \
                         .filter(lambda x: x is not None)

police_header = police_stations.first()
police_stations = police_stations.filter(lambda row: row != police_header).map(parse_csv) \
                                 .filter(lambda x: x is not None)

crime_data = crime_data1.union(crime_data2)

# Filter crime data for weapons starting with '1'
crime_data_guns = crime_data.filter(lambda row: row[16].startswith('1') if row[16] else False)

# Filter null island
crime_data_guns = crime_data_guns.filter(lambda row: row[26] != '0' or row[27] != '0')

# Strip Area column from initial 0
crime_data_guns = crime_data_guns.map(
                        lambda row: tuple(row[i].lstrip('0') if i == 4 else row[i] for i in range(len(row))))

# ---------------------------------------------------------------------------------------------#
# Regular join

# Map the police data and crime data to key-value pairs
#crime_data_guns_with_keys = crime_data_guns.map(lambda row: (int(row[4]), row))  # AREA is index 4
#police_stations_with_keys = police_stations.map(lambda row: (int(row[5]), row))  # PREC is index 5

# Join the dataframes - normal way
#joined_rdd = crime_data_guns_with_keys.join(police_stations_with_keys)

#---------------------------------------------------------------------------------------------#
# Repartition join

rdd_crime = crime_data_guns.map(lambda x: (x[4], ('crime', x)))
rdd_police = police_stations.map(lambda x:(x[5], ('police', x)))


repartition_rdd = rdd_crime.union(rdd_police).groupByKey()

def process_tags(key,values):
      values_police = [v for tag, v in values if tag== 'police']
      values_crime = [v for tag, v in values if tag == 'crime']
      return [(key, (r, l)) for r in values_crime for l in values_police]

joined_rdd = repartition_rdd.flatMap(lambda x: process_tags(x[0], list(x[1])))

#---------------------------------------------------------------------------------------------#

# Calculate distance and add as a new column
joined_with_distance_rdd = joined_rdd.map(lambda row: (row[0], (row[1][1][3],
                                                      get_distance(float(row[1][0][26]), float(row[1][0][27]),
                                                                   float(row[1][1][1]), float(row[1][1][0])))))

# Group by division, count incidents, and calculate average distance
division_rdd = joined_with_distance_rdd.map(lambda row: (row[0], (row[1][0],1, row[1][1]))) \
                                        .reduceByKey(lambda a, b: (a[0], a[1] + b[1], a[2] + b[2]))

# Calculate the average distance
division_result_rdd = division_rdd.map(lambda row: (row[1][0], row[1][1], round(row[1][2] / row[1][1], 4)))


# Sort by incidents_total in descending order
sorted_division_result_rdd = division_result_rdd.sortBy(lambda row: row[1], ascending=False)

# Collect and show the result
result = sorted_division_result_rdd.collect()

number_of_elements = len(result)
#print("Number of elements in the result list:", number_of_elements)
# Display the result
for row in result:
    print(f"Division: {row[0]}, Incidents Total: {row[1]}, Average Distance: {row[2]} km")

# Stop the SparkContext
sc.stop()
