import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    '''
    - Read songs data from JSON song file 'filepath'
    - Perform ETL on each single song file and load a single record into songs and artists table.
    '''
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = (df.values[0][7],df.values[0][8],df.values[0][0],df.values[0][9],df.values[0][5])
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df.loc[0, ['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values.tolist()
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    
    '''
    - Read log data from JSON log filepath and then filtered by 'NextSong' action.
    - Convert the timestamp to datetime before inserting data into time table.
    - Perform ETL on log file and load a single record into time, users and songplay table.
    
    '''
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    
    # insert time data records
    time_data = (t, t.dt.hour, t.dt.day, t.dt.week, t.dt.month, t.dt.year, t.dt.weekday)
    column_labels = ('start_time', 'hour', 'day', 'week of year', 'month',' year', 'weekday')
    time_df = pd.DataFrame.from_dict(dict(zip(column_labels , time_data)))

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df.loc[:,['userId','firstName', 'lastName','gender','level']]
    
    # insert user records
    
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    df['ts'] = pd.to_datetime(df['ts'], unit='ms') #convert timestamp to datetime
    
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (index, row.ts, int(row.userId), row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)



def process_data(cur, conn, filepath, func):
    '''
    - Iterate and process all files from directory
    '''
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
    '''
    - Connect the ETL pipeline with the database, and process the data files with process_data function
    '''
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()