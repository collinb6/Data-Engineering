import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

ARN = config.get("IAM_ROLE", "ARN")
LOG_DATA = config.get("S3", "LOG_DATA")
LOG_JSONPATH = config.get("S3", "LOG_JSONPATH")
SONG_DATA = config.get("S3", "SONG_DATA")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"

staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events (
        artist VARCHAR,
        auth VARCHAR,
        firstName VARCHAR,
        gender VARCHAR,
        itemInSession INT,
        lastName VARCHAR,
        length FLOAT,
        level VARCHAR,
        location VARCHAR,
        method VARCHAR,
        page VARCHAR,
        registration FLOAT,
        sessionId INT,
        song VARCHAR,
        status INT,
        ts BIGINT,
        userAgent VARCHAR,
        userId VARCHAR
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        artist_id VARCHAR PRIMARY KEY,
        artist_latitude FLOAT,
        artist_location TEXT,
        artist_longitude FLOAT,
        artist_name VARCHAR,
        duration FLOAT,
        num_songs INT,
        song_id VARCHAR,
        title VARCHAR,
        year INT
    );
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id bigint identity(0, 1)  PRIMARY KEY,
        start_time  TIMESTAMP,
        user_id     VARCHAR,
        level       VARCHAR,
        song_id     VARCHAR,
        artist_id   VARCHAR,
        session_id  VARCHAR,
        location    VARCHAR,
        user_agent  VARCHAR
    );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id VARCHAR PRIMARY KEY, 
        first_name VARCHAR, 
        last_name VARCHAR, 
        gender VARCHAR, 
        level VARCHAR
    )
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id VARCHAR PRIMARY KEY, 
        title VARCHAR, 
        artist_id VARCHAR, 
        year VARCHAR
    )
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id VARCHAR PRIMARY KEY, 
        name VARCHAR, 
        location VARCHAR, 
        lattitude VARCHAR, 
        longitude VARCHAR
    )
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time(
        start_time    timestamp sortkey,
        hour            int     NOT NULL,
        day             int     NOT NULL,
        week            int     NOT NULL,
        month           int     NOT NULL,
        year            int     NOT NULL,
        weekday         int     NOT NULL,
        PRIMARY KEY (start_time)
    );
""")

# STAGING TABLES
staging_events_copy = ("""COPY staging_events FROM {}
credentials {}
region 'us-west-2'
format json as {}
""").format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("""COPY staging_songs FROM {}
credentials {}
region 'us-west-2'
json 'auto'
""").format(SONG_DATA, ARN)

# FINAL TABLES

songplay_table_insert = ("""insert into songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
    select distinct
    timestamp 'epoch' + se.ts * interval '1 second' AS start_time,
    se.userid, se.level, ss.song_id, ss.artist_id, se.sessionid, se.location, se.useragent from staging_events se
    inner join staging_songs ss
    on (se.song = ss.title and se.artist = ss.artist_name)
""")

user_table_insert = ("""insert into users
    select distinct userid,firstname,lastname,gender,level from staging_events
""")

song_table_insert = ("""insert into songs
    select song_id, title, year, duration from staging_songs
""")

artist_table_insert = ("""insert into artists 
    select distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude from staging_songs
""")

time_table_insert = (
"""insert into time
    select distinct
        TIMESTAMP 'epoch' + (ts/1000) * INTERVAL '1 second' as start_time,
        EXTRACT(hour from start_time) as hour,
        EXTRACT(day from start_time) as day,
        EXTRACT(week from start_time) as week,
        EXTRACT(month from start_time) as month,
        EXTRACT(year from start_time) as year,
        EXTRACT(weekday from start_time) as weekday        
        from staging_events se
        where se.page = 'NextSong';
""")

# QUERY LISTS

#create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, #user_table_create, song_table_create, artist_table_create, time_table_create]
#drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, #song_table_drop, artist_table_drop, time_table_drop]
#insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

copy_table_queries = [staging_events_copy, staging_songs_copy]
create_table_queries = [staging_events_table_create, staging_songs_table_create, song_table_create, songplay_table_create,artist_table_create,user_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, artist_table_drop, song_table_drop,songplay_table_drop,time_table_drop] 
insert_table_queries = [artist_table_insert,user_table_insert,song_table_insert,songplay_table_insert,time_table_insert]