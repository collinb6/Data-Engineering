# Introduction
In this project I use Postgres and Python to build an ETL pipeline for a fictional music streaming startup called 'Sparkify'.

Sparkify stores their data in directories containing two types of JSON files:
    log_data: log files of user activity - (such as log-ins, log-outs, song-plays)
    song_data: metadata on the catalogue of songs available in the app

I create a database schema and ETL pipeline that converts this data into a Postgres database, with tables designed to optimize queries for song play analysis.


# Database schema
I create a star schema database, consisting of a single fact table and a series of dimension tables.

The fact table 'songplays' is derived from log_data - it consists of a chronological list of song plays across all users in the system.

Four dimension tables supplement the fact table. 
The 'artists' and 'songs' dimension tables are derived from song_data. They consists of a list of all artists and songs in the system respectively, with details such as song title, artist name, year of release etc.
The 'time' and 'users' dimension tables are derived from log_data. The 'time' table constists of the timestamps of the records in songplays broken into units such as hour, day, week etc. The 'users' table consists of the users in the app with details such as first and last name, gender, subscription type etc.


# Python Scripts
The project has three main scripts that can be run from the command line. 
'create_tables.py' - creates the database and defines the fact and dimension tables
'etl.py' - this is the extract/transform/load process, which reads the raw JSON data, and transforms it to our new schema, and inserts records into our fact and dimension tables
'sql_queries.py' - stores all of the sql queries (create/drop/insert) used by 'create_tables.py' and 'etl.py'. 