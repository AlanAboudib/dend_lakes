# Project: Data Lakes with Spark
## The Data Engineering NanoDegree
--------------

### 1- Summary of the Project

A company called Sparkify, in the music streaming industry, needs to build some analytics out of the data it is collecting from uses. Such data take the form of song details, user details, artist details and logs of when users listen to some songs.

Such analytics would be useful to better understand users behavious and thus be able to provide a better service.

The problem is that such data is stored as .json files that are not easy to manipulate directly to create analytics and visualisations.

The purpose of this project is to help the company organize their data in a column-orient data store  (Parquet in our case). Such a database is easier to query to create customizable, fast analytics and eventually visualisations.

The ETL process, is the process that moves data from the .json files using SPARK then to Parquet files


### 2- Project folder contents

The submitted folder contains a few files and scripts that implements the process of creating the database and the process of populating the database (ETL). Here is an overview of these files:

- **etl.py:** The code to load data from JSON files from S3 using Spark, and to write data to Parquet.
- **dl.cfg:** Credentials for S3 destination table should be put here.

### 3. The database schema

Since we are using parquet files, then each table is stored using a partition key corresponding to the query we expect to use:

songs table: partition key is 'year' and 'artist_id'
artists table:  partition key is 'artist_is'
users table:  partition key is 'user_id'
time table:  partition key is 'timestamp'
songplays: partition key is 'year' and 'month'

### 4. Running the scripts:

You can run the etl.py script locally if you use Spark on your own machine as

```
$ python etl.py
```

Do not forget to put aws access ID and secret access key in the dl.cfg file

### 5. A final word

This project was a great occasion for me to appreciate why ETL is used in practice. It provided a more realistic usecase on applying ETL to a more realistic scenario that the first two projects.
