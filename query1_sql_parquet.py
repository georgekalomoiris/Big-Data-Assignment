from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, date_format, split, col
from pyspark.sql.window import Window
from pyspark.sql.functions import rank

if __name__ == '__main__':
    spark = SparkSession.builder \
        .appName("query1_sql_parquet") \
        .getOrCreate()

    # Read the CSV file using the Spark session
    #df1 = spark.read.csv('data/Crime_Data_from_2010_to_2019.csv', header=True, inferSchema=True)
    #df2 = spark.read.csv('data/Crime_Data_from_2020_to_Present.csv', header=True, inferSchema=True)

    df1 = spark.read.parquet('hdfs://master:9000/home/user/ergasia/Crime_Data_from_2010_to_2019.parquet', header=True, inferSchema=True)
    df2 = spark.read.parquet('hdfs://master:9000/home/user/ergasia/Crime_Data_from_2020_to_Present.parquet', header=True, inferSchema=True)

    # Merge the dataframes into one
    df = df1.union(df2)

    # Create a temporary view from the dataframe
    df.createOrReplaceTempView("crime_data")

    # SQL query to handle date type for 'DATE OCC' and to format 'DateOccured' to 'yyyy-MM'
    query1 = """
        SELECT 
            DATE_FORMAT(TO_TIMESTAMP(`DATE OCC`, 'MM/dd/yyyy hh:mm:ss a'), 'yyyy-MM') as DateOccured
        FROM 
            crime_data
        """

    df_formatted = spark.sql(query1)
    df_formatted.createOrReplaceTempView("formatted_data")

    # SQL query to group by 'DateOccured' and count the frequency of each unique entry
    query2 = """
        SELECT 
            DateOccured, 
            COUNT(*) as count
        FROM 
            formatted_data
        GROUP BY 
            DateOccured
        """

    frequency_df = spark.sql(query2)
    frequency_df.createOrReplaceTempView("frequency_data")

    # SQL query to split 'DateOccured' into 'year' and 'month' columns
    query3 = """
        SELECT 
            SPLIT(DateOccured, '-')[0] as year,
            SPLIT(DateOccured, '-')[1] as month,
            count
        FROM 
            frequency_data
        """

    split_df = spark.sql(query3)
    split_df.createOrReplaceTempView("split_data")

    # SQL query to rank the months within each year based on the crime total
    query4 = """
        SELECT 
            year, 
            month, 
            count as crime_total,
            RANK() OVER (PARTITION BY year ORDER BY count DESC) as ranking
        FROM 
            split_data
        """

    ranked_df = spark.sql(query4)
    ranked_df.createOrReplaceTempView("ranked_data")

    # SQL query to filter to keep only the top 3 months for each year and order the results by year and ranking
    query5 = """
        SELECT 
            year, 
            month, 
            crime_total,
            ranking
        FROM 
            ranked_data
        WHERE 
            ranking <= 3
        ORDER BY 
            year, 
            ranking
        """

    top3_df = spark.sql(query5)

    # Show the resulting dataframe
    top3_df.show()

    spark.stop()
