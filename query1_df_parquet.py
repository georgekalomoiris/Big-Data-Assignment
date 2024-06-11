from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, date_format, split, col
from pyspark.sql.window import Window
from pyspark.sql.functions import rank

if __name__ == '__main__':
    spark = SparkSession.builder \
        .appName("query1_df_parquet") \
        .getOrCreate()

    # Read the CSV file using the Spark session
    #df1 = spark.read.csv('data/Crime_Data_from_2010_to_2019.csv', header=True, inferSchema=True)
    #df2 = spark.read.csv('data/Crime_Data_from_2020_to_Present.csv', header=True, inferSchema=True)

    df1 = spark.read.parquet('hdfs://master:9000/home/user/ergasia/Crime_Data_from_2010_to_2019.parquet', header=True, inferSchema=True)
    df2 = spark.read.parquet('hdfs://master:9000/home/user/ergasia/Crime_Data_from_2020_to_Present.parquet', header=True, inferSchema=True)

    # Merge the dataframes into one
    df = df1.union(df2)

    # Handle date type for 'DATE OCC'
    df = df.withColumn('DateOccured', to_timestamp(df['DATE OCC'], 'MM/dd/yyyy hh:mm:ss a'))
    df = df.withColumn('DateOccured', date_format(df['DateOccured'], 'yyyy-MM'))

    # Group by 'DateOccured' and count the frequency of each unique entry
    frequency_df = df.groupby('DateOccured').count()

    # Split 'DateOccured' into 'year' and 'month' columns
    frequency_df = frequency_df.withColumn('year', split(col('DateOccured'), '-').getItem(0))
    frequency_df = frequency_df.withColumn('month', split(col('DateOccured'), '-').getItem(1))

    # Select the columns in the desired order
    frequency_df = frequency_df.select('year', 'month', 'count')

    # Define a window specification to rank the months within each year based on the crime total
    window_spec = Window.partitionBy('year').orderBy(col('count').desc())

    # Add a ranking column based on the window specification
    frequency_df = frequency_df.withColumn('ranking', rank().over(window_spec))

    # Filter to keep only the top 3 months for each year
    top3_df = frequency_df.filter(col('ranking') <= 3)

    # Order the results by year and ranking
    top3_df = top3_df.orderBy('year', 'ranking')
    top3_df = top3_df.withColumnRenamed('count', 'crime_total')

    # Show the resulting dataframe
    top3_df.show()

    spark.stop()
