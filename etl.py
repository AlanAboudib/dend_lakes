import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """Create a SparkSession object
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """reads the song_data json files and create parquet files 
       corresponding to songs and artists tables on s3

       Args:
          spark: a SparkSession object.
          input_data (str): path to the s3 bucket where json files are situated.
          output_data (str): path to the s3 bucket where parquet files are stored
    """
    # get filepath to song data file
    song_data = os.path.join(input_data, 'song_data/*/*/*/*.json')

    
    # read song data file
    df = spark.read.json(song_data)
    

    # extract columns to create songs table
    songs_table = df['song_id', 'title', 'artist_id', 'year', 'duration']
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").mode('overwrite').parquet(os.path.join(output_data, 'songs'))

    # extract columns to create artists table
    artists_table = df.select('artist_id',
                              col('artist_name').alias('name'),
                              col('artist_location').alias('location'),
                              col('artist_latitude').alias('latitude'),
                              col('artist_longitude').alias('longitude')
    )

    # write artists table to parquet files
    artists_table.write.partitionBy("artist_id").mode("overwrite").parquet(os.path.join(output_data, 'artists'))


def process_log_data(spark, input_data, output_data):
    """reads the log_data json files and create parquet files 
       corresponding to time, users and songplays tables on s3

       Args:
          spark: a SparkSession object.
          input_data (str): path to the s3 bucket where json files are situated.
          output_data (str): path to the s3 bucket where parquet files are stored
    """
    
    # get filepath to log data file
    log_data = os.path.join(input_data, 'log_data/*/*/*.json')


    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df[df.page == 'NextSong']

    # extract columns for users table    
    users_table = df.select(col('userId').alias('user_id'),
                            col('firstName').alias('first_name'),
                            col('lastName').alias('last_name'),
                            'gender',
                            'level'
                           )

    # write users table to parquet files
    users_table.write.partitionBy("user_id").mode('overwrite').parquet(os.path.join(output_data, 'users'))

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: int((int(x)/1000)))
    df = df.withColumn('timestamp', get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x))
    df = df.withColumn('date', get_datetime(df.timestamp))
    
    # extract columns to create time table
    time_table = df.select(col('timestamp').alias('start_time'),
                           hour('date').alias('hour'),
                           dayofmonth('date').alias('day'),
                           weekofyear('date').alias('week'),
                           month('date').alias('month'),
                           year('date').alias('year'),
                           date_format('date','E').alias('weekday')
    )
    #start_time, hour, day, week, month, year, weekday
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month').mode('overwrite').parquet(os.path.join(output_data, 'time'))

    # read in song data to use for songplays table
    song_df = spark.read.parquet(os.path.join(input_data, 'songdata/*/*/*/*.json'))

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(dong_df,
                              (df.artist == song_df.artist_name) & (df.title == song_df.song)
    ).select(col("start_time"),
             col("userId").alias("user_id"),
             col("level")
             col("song_id"),
             col("artist_id"),
             col("sessionId").alias('session_id'),
             col("artist_location").alias("location"),
             col("userAgent").alias("user_agent")
    )

    # write songplays table to parquet files partitioned by year and month
    songplays_table = songplays.write.partitionBy('year', 'month').mode('overwrite').parquet(os.path.join(output_data, 'songplays'))
    

def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://aaaodbaoel-dend-bucket/"
    
    #process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
