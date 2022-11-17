import psycopg2
import pandas as pd


# Example sql queries

## Find the top 5 most listened to songs
sql1 = """SELECT title, COUNT(*) AS playcount FROM songplays LEFT JOIN songs ON songplays.song_id = songs.song_id GROUP BY title ORDER BY playcount DESC LIMIT 5;
"""

## Find the top 5 listening sessions by total session length

sql2 = """select session_id, MAX(start_time) - MIN(start_time) AS session_length from songplays group by session_id order by session_length DESC limit 5;
"""

def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    # Top 5 most listened to songs
    print('Top 5 listened-to songs')
    cur.execute(sql1)
    results = cur.fetchall()
    print(results)
    
    # Top 5 listening sessions
    print('Top 5 listening sessions by session length')
    cur.execute(sql2)
    results = cur.fetchall()
    print(results)
    
    conn.close()


if __name__ == "__main__":
    main()