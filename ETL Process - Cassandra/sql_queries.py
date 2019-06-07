### DROPPING table queries are being used to drop all of the tables
# Drop Tables
drop_music_session_items = ("""
    DROP TABLE IF EXISTS music_session_items
""")

drop_user_sessions = ("""
    DROP TABLE IF EXISTS user_sessions
""")

drop_song_sessions = ("""
    DROP TABLE IF EXISTS song_sessions
""")


### CREATE table queries are being used to create the necessary tables so it can be used later during the analytical portion
# Create Tables

## TO-DO: Query 1:  Give me the artist, song title and song's length in the music app history that was heard during \
## sessionId = 338, and itemInSession = 4
create_music_session_items = ("""
    CREATE TABLE IF NOT EXISTS music_session_items
    (   
        session_id int, 
        session_item_id int, 
        artist_name text, 
        song_title text, 
        duration decimal, 
        PRIMARY KEY(session_id, session_item_id)
    )
""")

## TO-DO: Query 2: Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name)\
## for userid = 10, sessionid = 182
create_user_sessions = ("""
    CREATE TABLE IF NOT EXISTS user_sessions
    (
        user_id int,
        session_id int,
        session_item_id int,
        artist_name text,
        song_title text,
        user_first_name text,
        user_last_name text,
        PRIMARY KEY((user_id, session_id), session_item_id)
    ) WITH CLUSTERING ORDER BY (session_item_id ASC)
""")


## TO-DO: Query 3: Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'
create_song_sessions = ("""
    CREATE TABLE IF NOT EXISTS song_sessions
    (
        user_id int,
        song_title text,
        user_first_name text,
        user_last_name text,
        PRIMARY KEY(song_title, user_id)
    )
""")

### Insert queries are used to insert all of the data into the system.
# Insert Queries

insert_music_session_items = ("""
    INSERT INTO music_session_items (
        session_id, 
        session_item_id, 
        artist_name, 
        song_title, 
        duration
    ) VALUES (%s, %s, %s, %s, %s)
""")

insert_user_sessions = ("""
    INSERT INTO user_sessions(
        user_id,
        session_id,
        session_item_id,
        artist_name,
        song_title,
        user_first_name,
        user_last_name 
    ) VALUES(%s, %s, %s, %s, %s, %s, %s)
""")

insert_song_sessions = ("""
    INSERT INTO song_sessions(
        user_id,
        song_title,
        user_first_name,
        user_last_name
    ) VALUES(%s, %s, %s, %s)
""")

drop_table_queries = [drop_music_session_items, drop_user_sessions, drop_song_sessions]
create_table_queries = [create_music_session_items, create_user_sessions, create_song_sessions]
insert_table_queries = [insert_music_session_items, insert_user_sessions, insert_song_sessions]