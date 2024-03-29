import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *

def convert_json_file_to_dataframe(filepath):
    """
    Description: Read the json file (song data or log data) via pandas and return it as data frame

    Arguments:
        filepath: data file path. 

    Returns:
        df: data frame read from the file
    """
    df = pd.read_json(filepath, lines = True)
    return df

def process_song_file(cur, filepath):
    """
    Description: This function can be used to read the file in the filepath (data/song_data)
    to get the song and artist info and used to populate the songs and artists dim tables.

    Arguments:
        cur: the cursor object. 
        filepath: song data file path. 

    Returns:
        None
    """
    # open song file
    df = convert_json_file_to_dataframe(filepath)

    # insert song record
    artist_id, artist_latitude, artist_location, artist_longitude, artist_name, duration, num_songs, song_id, title, year = df.values[0]
    song_data = [song_id, title, artist_id, year, duration]
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = [artist_id, artist_name, artist_location, artist_latitude, artist_longitude]
    cur.execute(artist_table_insert, artist_data)

def convert_time_data(timeSrc):
    """
    Description: This function can be used to convert the timestamp data to specified array
    
    Arguments:
        filepath: timestamp data. 

    Returns:
        time_data: converted time data represented as an array
    """
    time_data = []
    for time in timeSrc:
        time_data.append([time, time.hour, time.day, time.week, time.month, time.year, time.day_name()])
    return time_data

def process_log_file(cur, filepath):
    """
    Description: This function can be used to read the file in the filepath (data/log_data)
    to get the user and time info and used to populate the users and time dim tables.

    Arguments:
        cur: the cursor object. 
        filepath: log data file path. 

    Returns:
        None
    """
    # open log file
    df = convert_json_file_to_dataframe(filepath)

    # filter by NextSong action
    df = df[df['page']=='NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    
    # insert time data records
    time_data = convert_time_data(t)
    column_labels = ('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday')
    time_df = pd.DataFrame.from_records(time_data, columns=column_labels)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (index, pd.to_datetime(row.ts, unit='ms'), row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
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
    try:
        conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
        cur = conn.cursor()

        process_data(cur, conn, filepath='data/song_data', func=process_song_file)
        process_data(cur, conn, filepath='data/log_data', func=process_log_file)

        conn.close()
    except Exception as e:
        print(e)


if __name__ == "__main__":
    main()