import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """This function processes the data from the song .json files and inserts the records into the `songs` and `artists` tables, respectively.
    """
    # open song file using pandas read_json method, save as a dataframe
    df = pd.read_json(filepath, lines=True)

    # select fields to be added to the songs table, extract values from dataframe, and execute the insert command
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0]
    cur.execute(song_table_insert, song_data)

    # select fields to be added to the artists table, extract values from dataframe, and execute the insert command
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values[0]
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """This function processes the data from the log .json files and inserts the records into the `time`, `user`, and `songplay` tables, respectively.
    """
    # open log file using pandas read_json method, save as a dataframe
    df = pd.read_json(filepath, lines=True)

    # filter dataframe for records with page == 'NextSong'
    df = df[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')

    # insert time data records using pandas dt attributes
    time_data = (t, t.dt.hour, t.dt.day, t.dt.weekofyear, t.dt.month, t.dt.year, t.dt.weekday)
    column_labels = ('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday')
    time_df = pd.DataFrame.from_dict(dict(zip(column_labels, time_data)))

    # add rows incrementally to the `time` table
    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # select fields to be added to the users table, extract values from dataframe, and execute the insert command
    user_df = df[['userId','firstName','lastName','gender','level']]

    # add rows incrementally to the `time` table
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # add rows incrementally to the `songplays` table
    for index, row in df.iterrows():

        # get songid and artistid from song and artist tables -- these don't exist on the raw log files, so we'll need to query for associated songs/artists based on song title, artist name, and song duration
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        # if the above query finds a song/artist match, save them to the variables songid and artistid. Otherwise, set these variables to None
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (pd.to_datetime(row.ts, unit='ms'), row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """This function has the following structure:
    1) Loop through all subfolders in the data folder, find all .json files, and save their filespaths to a list called `all_files`.
    2) Print file count to console
    3) Apply the function argument (func -- either process_song_file or process_log_file defined above) to files in all_files iteratively (which commits data to the appropriate tables)
    4) Print file process success note as data is committed
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
    """This is the main function that runs all process defined above:
    1) Connect to the sparkifydb and initiate a cursor
    2) Apply the process_data function to the files in the song_data subfolder, using the process_song_file function defined above
    3) Apply the process_data function to the files in the log_data subfolder, using the process_log_file function defined above
    4) Close the connection for security
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    """Run the script"""
    main()
