import os
import glob
import psycopg2
import pandas as pd
import datetime
from sql_queries import *


def process_song_file(cur, filepath):
    """
    Takes a filepath to one of the song_data JSON files.
    It then inserts the records into the postgreSQL dimension tables 'songs'
    and 'artists'
    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[['song_id','title','artist_id','year','duration']]
    song_data = (song_data.values)[0]
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']]
    artist_data = (artist_data.values)[0]    
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    Takes a filepath to a log file and converts the JSON log file to a pandas dataframe.
    It then iterates through the dataframe and inserts records into the postgreSQL 'times' and 'users' dimension tables, 
    as well as the 'songplays' fact table.
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df.loc[df['page'] == "NextSong"]

    # convert timestamp column to datetime
    t = pd.Series([datetime.datetime.fromtimestamp(i) for i in (df['ts'])/1000])
    
    # insert time data records
    #time_data = 
    #column_labels = 
    time_df = pd.DataFrame({'start_time':t.dt.time,'hour':t.dt.hour,'day':t.dt.day,'week':t.dt.weekofyear,'month':t.dt.month,'year':t.dt.year,'weekday':t.dt.weekday_name})


    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId','firstName','lastName','gender','level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        # cur.execute(song_select, (row.song, row.artist, row.length))
        cur.execute(song_select, (row.artist, row.length, row.song))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    Recursively finds all files contained within the given filepath.
    For each file found it calls the function specified by the parameter 'func' (either 'process_song_file' or 'process_log_file'),
    with the file as the function's parameter
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """
    Establishes a connection with the sparkify database.
    It then calls 'process_data' on both the song_data and log_data directories
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()