from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession.builder \
        .appName("CSV to Parquet Converter") \
        .getOrCreate()

    # Read the CSV files
    df1 = spark.read.csv('hdfs://master:9000/home/user/ergasia/Crime_Data_from_2010_to_2019.csv', header=True, inferSchema=True)
    df2 = spark.read.csv('hdfs://master:9000/home/user/ergasia/Crime_Data_from_2010_to_2019.csv', header=True, inferSchema=True)

    # Save the DataFrames as Parquet files
    df1.write.parquet('hdfs://master:9000/home/user/ergasia/Crime_Data_from_2010_to_2019.parquet')
    df2.write.parquet('hdfs://master:9000/home/user/ergasia/Crime_Data_from_2020_to_Present.parquet')

    spark.stop()
