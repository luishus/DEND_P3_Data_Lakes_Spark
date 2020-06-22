import configparser
import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.types import TimestampType
from sql_queries import songs_table_query, artists_table_query, log_filtered_query, users_query, time_query, songplays_query

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    This function creates a session with Spark.
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    This function reads the songs JSON files from S3 and processes them with Spark. We separate the files into specific dataframes the represent the tables in our star schema model.
    Then, these tables are saved back to the output folder indicated by output_data parameter.
    
    Args:
        spark (SparkSession): Spark session.
        input_data (str): Directory where to find the JSON input files.
        output_data (str): Directory where to save parquet files.
    """
    # get filepath to song data file
    song_data = input_data + "song_data/A/A/A/*"
    #song_data = input_data + "song_data/*/*/*"
    
    # read song data fileS
    df = spark.read.json(song_data)
    df.createOrReplaceTempView("songs")
    
    # extract columns to create songs table
    songs_table = spark.sql(songs_table_query)
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").parquet(path = output_data + "/songs/songs.parquet", mode = "overwrite")

    # extract columns to create artists table
    artists_table = spark.sql(artists_table_query)
    # write artists table to parquet files
    artists_table.write.parquet(path = output_data + "/artists/artists.parquet", mode = "overwrite")


def process_log_data(spark, input_data, output_data):
    """
    This function reads the logs JSON files from S3 and processes them with Spark. We separate the files into specific dataframes the represent the tables in our star schema model.
    Then, these tables are saved back to the output folder indicated by output_data parameter.
    
    Args:
        spark (SparkSession): Spark session.
        input_data (str): Directory where to find the JSON input files.
        output_data (str): Directory where to save parquet files.
    """
    # get filepath to log data file
    log_data = input_data + "log_data/2018/11/*"
    #log_data = input_data + "log_data/*"
    
    # read log data file
    df = spark.read.json(log_data)
    
    df.createOrReplaceTempView("events")
    # filter by actions for song plays
    df = spark.sql(log_filtered_query)
    
    df.createOrReplaceTempView("events")
    # extract columns for users table
    users_table = spark.sql(users_query).dropDuplicates(['user_id', 'level'])
    # write users table to parquet files
    users_table.write.parquet(path = output_data + "/users/users.parquet", mode = "overwrite")
    
    # extract columns to create time table
    time_table = spark.sql(time_query)
       
    #create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.datetime.fromtimestamp(x / 1000), TimestampType())
    time_table = time_table.withColumn("start_time", get_timestamp(time_table.ts))
    time_table = time_table.drop('ts')
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").parquet(path = output_data + "/time/time_parquet", mode = "overwrite")
    
    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data + "/songs/songs.parquet")
    song_df.createOrReplaceTempView("songs")
    
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql(songplays_query)
    
    # replacing start_time column
    songplays_table = songplays_table.withColumn("start_time", get_timestamp(songplays_table.start_time))   
    
    # adding songplay_id column
    songplays_table = songplays_table.withColumn("songplay_id", monotonically_increasing_id())
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year', 'month').parquet(path = output_data + "/songplays/songplays_parquet", mode = "overwrite")


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://aws-udacity-spark/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)
    


if __name__ == "__main__":
    main()