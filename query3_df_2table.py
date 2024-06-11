from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, to_timestamp, desc, udf, first, date_format
from pyspark.sql.types import StringType
from pyspark.sql.functions import regexp_replace


# Initialize Spark session
spark = SparkSession.builder.appName("query3_df_both_tables").getOrCreate()

# Load the datasets
crime_data = spark.read.csv("hdfs://master:9000/home/user/ergasia/Crime_Data_from_2010_to_2019.csv", header=True, inferSchema=True)
revgecoding = spark.read.csv("hdfs://master:9000/home/user/ergasia/revgecoding.csv", header=True, inferSchema=True)
income_data = spark.read.csv("hdfs://master:9000/home/user/ergasia/LA_income_2015.csv", header=True, inferSchema=True)

# Define the descent code mapping
descent_mapping = {
    'A': 'Other Asian',
    'B': 'Black',
    'C': 'Chinese',
    'D': 'Cambodian',
    'F': 'Filipino',
    'G': 'Guamanian',
    'H': 'Hispanic/Latin/Mexican',
    'I': 'American Indian/Alaskan Native',
    'J': 'Japanese',
    'K': 'Korean',
    'L': 'Laotian',
    'O': 'Other',
    'P': 'Pacific Islander',
    'S': 'Samoan',
    'U': 'Hawaiian',
    'V': 'Vietnamese',
    'W': 'White',
    'X': 'Unknown',
    'Z': 'Asian Indian'
}

# Create a UDF to map the descent codes to their full descriptions
def map_descent_code(code):
    return descent_mapping.get(code, 'Unknown')

map_descent_code_udf = udf(map_descent_code, StringType())

# Step 1: Filter the crime data for the year 2015 and non-null Vict Descent
crime_data_filtered = crime_data.select(col('DATE OCC'), col('Vict Descent'), col('LAT'), col('LON'))
crime_data_filtered = crime_data_filtered.withColumn("DATE OCC", to_timestamp(col("DATE OCC"), "MM/dd/yyyy hh:mm:ss a"))
crime_data_filtered = crime_data_filtered.withColumn('DATE OCC',date_format(col('DATE OCC'),'yyyy'))
crime_data_filtered = crime_data_filtered.filter(col('DATE OCC')=='2015')

crime_data_filtered = crime_data_filtered.na.drop(subset=['Vict Descent']) 
revgecoding =  revgecoding.dropDuplicates(['LAT','LON'])

# Transform income data 
income_data = income_data.withColumn(
    "Estimated Median Income",
    regexp_replace("Estimated Median Income", "[^\d]", "").cast("int"))

# Step 2: Join crime data with revgecoding to get ZIP codes
crime_data_with_zip = crime_data_filtered.join(revgecoding, on=["LAT", "LON"], how="inner")
#crime_data_with_zip.explain("simple")


# Step 3: Join with income data to get estimated median income
crime_data_with_income = crime_data_with_zip.join(income_data, crime_data_with_zip["ZIPcode"] == income_data["ZIP Code"], "inner")
#crime_data_with_income.explain('simple')

# Step 4: Identify the top 3 highest and bottom 3 lowest income ZIP code

highest_income = crime_data_with_income.select('ZIPcode').distinct().orderBy(col('Estimated Median Income').desc()).limit(3)
lowest_income =  crime_data_with_income.select('ZIPcode').distinct().orderBy(col('Estimated Median Income')).limit(3)

# Turn them into list 
highest_list = [row['ZIPcode'] for row in highest_income.collect()]
lowest_list = [row['ZIPcode'] for row in lowest_income.collect()] 


# Step 5: Filter for relevant ZIP codes and group by Vict Descent
def get_descent_counts(zipcodes):
    filtered_data = crime_data_with_income.filter(col("ZIP Code").isin(zipcodes))
    descent_counts = filtered_data.withColumn("Vict Descent", map_descent_code_udf(col("Vict Descent")))
    descent_counts_grouped = descent_counts.groupBy("Vict Descent").count()
    descent_counts_sorted = descent_counts_grouped.orderBy(desc("count"))
    descent_counts_named =  descent_counts_sorted.withColumnRenamed("Vict Descent", "Victim Descent").withColumnRenamed("count","Total Victims")
    return descent_counts_named

highest_income_descent_counts = get_descent_counts(highest_list)
lowest_income_descent_counts = get_descent_counts(lowest_list)



print("Highest Income ZIP Codes Descent Counts:")
highest_income_descent_counts.show()

print("Lowest Income ZIP Codes Descent Counts:")
lowest_income_descent_counts.show()

spark.stop()
