DATABASE DESIGN:

This database for Sparkify was designed to store user usage data as well as catalog information about artists and songs. The data will be used to analyze customer trends and tastes. The data could be enriched to provide recommendations and custom playlists for users based on their listening history. 

HOW TO:

Open a terminal and run the command `python create_tables.py` to initalize the database and create the core tables. Then, run the command `etl.py` to populate the database with data from the user and song logs. 

FILES:

Housed in the `data` folder are two subfolders. One contains a user log with session-level data captured from the Sparkify application with session identifying information as well as user details. The other contains information about artists and songs. 


SCHEMA:

A star schema design was chosen to enable easy querying of session and song level listening data, particularly around session duration and song/artist frequencies. 

EXAMPLE QUERIES:

Run the command `python test_queries.py` to see some analytical examples using this database. 