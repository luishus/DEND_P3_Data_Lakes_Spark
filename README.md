# Project 4: Data Lake

## Summary

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. The data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

The purpose is build an ETL pipeline for a data lake hosted in S3 that extracts data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables.
The spark process will be deploy on a vluster using AWS


## Schema design and ETL pipeline

The star schema has 1 *fact* table (songplays), and 4 *dimension* tables (users, songs, artists, time).


### Source files

#### log_data

|   Column       |            Type             | 
| -------------- | --------------------------- | 
| artist         | string                      | 
| auth           | string                      | 
| firstName      | string                      | 
| gender         | string                      | 
| itemInSession  | long                        | 
| lastName       | string                      | 
| length         | double                      | 
| level          | string                      | 
| location       | string                      |
| method         | string                      | 
| page           | string                      | 
| registration   | double                      | 
| sessionId      | long                        | 
| song           | string                      | 
| status         | long                        | 
| ts             | long                        | 
| userAgent      | string                      | 
| userId         | string                      | 


#### song_data

|   Column          |            Type             | 
| ----------------- | --------------------------- | 
| num_songs         | long                        | 
| artist_id         | string                      | 
| artist_latitude   | double                      | 
| artist_longitude  | double                      | 
| artist_location   | string                      | 
| artist_name       | string                      | 
| song_id           | string                      | 
| title             | string                      | 
| duration          | float                       | 
| year              | long                        |



### Fact tables

#### Songplays

Records in log data associated with song plays.

|   Column    |            Type             | 
| ----------- | --------------------------- | 
| songplay_id | long                        | 
| start_time  | timestamp                   | 
| user_id     | string                      | 
| level       | string                      | 
| song_id     | string                      | 
| artist_id   | string                      | 
| session_id  | long                        | 
| location    | string                      | 
| user_agent  | string                      | 
| month       | integer                     | 
| year        | integer                     | 

Primary key: songplay_id

### Dimension tables

#### Users

Users in the app.

|   Column   |       Type        | 
| ---------- | ----------------- | 
| user_id    | string            | 
| first_name | string            | 
| last_name  | string            | 
| gender     | string            | 
| level      | string            | 

Primary key: user_id

#### Songs

Songs in music database.

|  Column   |         Type          |
| --------- | --------------------- |
| song_id   | string                |
| title     | string                |
| artist_id | string                |
| year      | long                  |
| duration  | double                |

Primary key: song_id

#### Artists

Artists in music database.

|  Column   |         Type          |
| --------- | --------------------- |
| artist_id | string                |
| name      | string                |
| location  | string                |
| latitude  | double                |
| longitude | double                |

Primary key: artist_id

#### Time

Timestamps of records in songplays.

|   Column   |            Type             | 
| ---------- | --------------------------- | 
| start_time | timestamp                   | 
| hour       | integer                     | 
| day        | integer                     | 
| week       | integer                     | 
| month      | integer                     | 
| year       | integer                     | 
| weekday    | integer                     | 

Primary key: start_time


Extract, transform, load processes in **etl.py** populate the **songs** and **artists** tables with data derived from the JSON song files, `data/song_data`. Processed data derived from the JSON log files, `data/log_data`, is used to populate **time** and **users** tables. A `SELECT` query collects song and artist id from the **songs** and **artists** tables and combines this with log file derived data to populate the **songplays** fact table.


## Run

Run the scripts to generate the set of dimensional tables:

```
./etl.py
```

Test file for `etl.py`:

```
./p4-aws-datalake.ipynb
```

