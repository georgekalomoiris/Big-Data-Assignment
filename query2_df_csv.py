from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

# Initialize Spark session
spark = SparkSession.builder \
    .appName("query2_df_csv") \
    .getOrCreate()

# Load the CSV files
df1 = spark.read.csv('hdfs://master:9000/home/user/ergasia/Crime_Data_from_2010_to_2019.csv', header=True, inferSchema=True)
df2 = spark.read.csv('hdfs://master:9000/home/user/ergasia/Crime_Data_from_2020_to_Present.csv', header=True, inferSchema=True)
df = df1.union(df2)


# Categorize the time into parts of the day
df = df.withColumn('TIME OCC',
                   when((col('TIME OCC')<1200) & (col('TIME OCC')>=500), 'morning')
                   .when((col('TIME OCC')>=1200) & (col('TIME OCC')<1700), 'afternoon')
                   .when((col('TIME OCC')>=1700) & (col('TIME OCC')<2100), 'evening')
                   .otherwise('night')
                  )

# Select only the required columns
df = df.filter(col('Premis Cd') == 101)
df = df.select('TIME OCC')
df = df.groupby('TIME OCC').count()
df = df.select('TIME OCC','count').orderBy(col('count').desc())

# Show the DataFrame to verify the changes
df.show()

spark.stop()
