import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *

"""
    At the top, we have imported:
    1. os -- this helps us to navigate through the necessary folders... Think of it as your ls/cd/mkdir options in your terminal
    2. glob -- this helps us to gather all of the file names and put it into a list
    3. psycopg2 -- the necessary connection to the DB was created using this library
    4. pandas -- we use Pandas to massage the data and clean it up as well at the same time
    5. sql_queries -- this is actually an import of our very own sql_queries.py. We passed all of our queries by doing this
"""

def process_song_file(cur, filepath):
    
    """
    Description: This function can be used to read the file in the filepath (data/song_data)
    to get the song and artist info.

    Arguments:
        cur: the cursor object. 
        filepath: log data file path. 

    Returns:
        None
    """
    
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[['song_id','title','artist_id','year','duration']].values.tolist()[0]
    try:
        cur.execute(song_table_insert, song_data)
    except psycopg2.Error as e:
        print("Error: Inserting song Rows")
        print(e)
        
    
    # insert artist record
    artist_data = df[['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']].values.tolist()[0]
    try:
        cur.execute(artist_table_insert, artist_data)
    except psycopg2.Error as e:
        print("Error: Inserting artist Rows")
        print(e)


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
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    df['ts'] = pd.to_datetime(df['ts'], unit='ms')
    t = df
    
    # insert time data records
    time_data = list([x, x.hour, x.day, x.weekofyear, x.month, x.year, x.dayofweek] for x in t['ts'])
    column_labels = list(['timestamp','hour','day','weekofyear','month','year','weekday'])
    time_df = pd.DataFrame(data=time_data, columns=column_labels)

    for i, row in time_df.iterrows():
        try:
            cur.execute(time_table_insert, list(row))
        except psycopg2.Error as e:
            print("Error: Inserting time Rows")
            print(e)
    
    
    # load user table
    user_df = df[['userId','firstName','lastName','gender','level']].drop_duplicates()

    # insert user records
    for i, row in user_df.iterrows():
        #user_id, first_name, last_name, gender, level
        try:
            cur.execute(user_table_insert, list(row))
        except psycopg2.Error as e:
            print("Error: Inserting user Rows")
            print(e)

            
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
        # start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
        songplay_data = [row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent.replace('"','')]
        try:
            cur.execute(songplay_table_insert, songplay_data)
        except psycopg2.Error as e:
            print("Error: Inserting songplay Rows")
            print(e)


def process_data(cur, conn, filepath, func):
   
    """
    Description: This function can be used to read the file in the filepath (data/log_data)
    to get the user and time info and used to populate the users and time dim tables.

    Arguments:
        cur: the cursor object. 
        conn: connection to DB.
        filepath: log data file path. 
        func: the function that you'd like to run. (really cool feature btw!)

    Returns:
        None
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
    #1 The main starts the connection to the database via psycopg2 and create the cursor as well as the connection
    #2 Once the connection is established, we use the process_data function and pass the cursor, connection, filepath and the function that 
        we'd like to run
    #3 From there, each of the necessary ETL process runs one after another
    """
    
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
