from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import Row
import csv
from io import StringIO

# Initialize Spark session and context
sc = SparkSession.builder \
    .appName("query2_rdd_csv") \
    .getOrCreate() \
    .sparkContext



# special parser because some rows have commas  as part of their content
def parse_csv(line):
    sio = StringIO(line)
    reader = csv.reader(sio)
    return next(reader)

# Load the CSV files as RDDs
data2 = sc.textFile('hdfs://master:9000/home/user/ergasia/Crime_Data_from_2020_to_Present.csv')
data1 = sc.textFile('hdfs://master:9000/home/user/ergasia/Crime_Data_from_2010_to_2019.csv')

# Extract the header
header1 = data1.first()
header2 = data2.first()

# Filter out the header and split the columns
data1 = data1.filter(lambda row: row != header1).map(parse_csv)
data2 = data2.filter(lambda row: row != header2).map(parse_csv)

# Union the two datasets
data = data1.union(data2)

# Define a function to categorize the time
def categorize_time(time_occ):
    time_occ = int(time_occ)
    if 500 <= time_occ < 1200:
        return 'morning'
    elif 1200 <= time_occ < 1700:
        return 'afternoon'
    elif 1700 <= time_occ < 2100:
        return 'evening'
    else:
        return 'night'

# Filter for Premis Cd == 101 and map the time categories
filtered_data = data.filter(lambda row: row[14] == '101')
time_data = filtered_data.map(lambda row: (categorize_time(row[3]), 1))


# Count occurrences by time category
time_counts = time_data.reduceByKey(lambda a, b: a + b)

# Convert to a DataFrame and show the result
time_counts_df = time_counts.map(lambda row: Row(time=row[0], count=row[1])).toDF()
time_counts_df.orderBy(time_counts_df['count'].desc()).show()

# Stop the Spark context
sc.stop()
