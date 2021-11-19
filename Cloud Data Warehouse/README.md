## Udactity Data Engineer Nanodegree Project: Data Warehouse

### Introduction
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

The objective here is to build an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to.

### Project Description
In this project, I apply what I've learned about data warehouses and AWS to build an ETL pipeline for a database hosted on Redshift. The project involves loading data from S3 to staging tables on Redshift and executing SQL statements that create the analytics tables from these staging tables.


### Data
Sparkify currently stores all of their data in JSON format in S3 storage.
There are two types of files:
    log_data: log files of user activity - (such as log-ins, log-outs, song-plays)
    song_data: metadata on the catalogue of songs available in the app


### Database schema
I load the sparkify data from S3 into two staging tables in my Redshift instance, `staging_events` and `staging_songs`. From these staging tables I use SQL statements to create a star schema database consisting of a single fact table, `songplays`, and several other dimenstion tables `users` `songs` `artists` `time`. 


### Python Scripts
The project has three main scripts that can be run from the command line. 
- `create_tables.py` - creates the database and defines the fact and dimension tables
- `etl.py` - this is the extract/transform/load process, which reads the raw JSON data, and transforms it to our new schema, and inserts records into our fact and dimension tables
- `sql_queries.py` - stores all of the sql queries (create/drop/insert) used by 'create_tables.py' and 'etl.py'. 
- `dwh.cfg` is a configuration file that has details for my amazon redshift instance

- the `create_tables.py` and `etl.py` scripts can be run from a python process, or from the notebook `main.inpynb`