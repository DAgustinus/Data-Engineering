# DROP TABLES

"""
The DROP tables are used to check if the tables are exist, and if they do, they will drop them.
The 'if exists' option allows us to check without any consequence of having an error
"""

songplay_table_drop = "drop table if exists songplays;"
user_table_drop = "drop table if exists users;"
song_table_drop = "drop table if exists songs;"
artist_table_drop = "drop table if exists artists;"
time_table_drop = "drop table if exists time;"

# CREATE TABLES

"""
Below are the table creation for all of the necessary tables for this project.
Each tables have a Primary key to make sure that they have a unique key for each row 
"""

songplay_table_create = (""" 
    create table if not exists songplays
    (songplay_id serial PRIMARY KEY NOT NULL, start_time timestamp NOT NULL, user_id int NOT NULL, level text, song_id text, artist_id text, session_id int, location text, user_agent text, UNIQUE(user_id, song_id, artist_id, session_id))
""")

user_table_create = ("""
    create table if not exists users
    (user_id int PRIMARY KEY NOT NULL, first_name text, last_name text, gender text, level text, UNIQUE(user_id))
""")

song_table_create = ("""
    create table if not exists songs
    (song_id text PRIMARY KEY NOT NULL, title text, artist_id text, year int, duration numeric, UNIQUE(song_id))
""")

artist_table_create = ("""
    create table if not exists artists
    (artist_id text PRIMARY KEY, name text, location text, lattitude float, longitude float, UNIQUE(artist_id))
""")

time_table_create = ("""
    create table if not exists time
    (start_time timestamp PRIMARY KEY NOT NULL, hour int, day int, week int, month text, year int, weekday text, UNIQUE(start_time))
""")

# INSERT RECORDS

"""
One of the most crucial part of the ETL is to make sure that the INSERTS are working properly. Below you will see
what I've done for the inserts
"""

songplay_table_insert = ("""
    insert into songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    values(%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING;
""")

user_table_insert = ("""
        insert into users (user_id, first_name, last_name, gender, level)
    values(%s,%s,%s,%s,%s) ON CONFLICT(user_id) DO UPDATE SET level = excluded.level;
""")

song_table_insert = ("""
    insert into songs (song_id, title, artist_id, year, duration) values(%s,%s,%s,%s,%s)
    ON CONFLICT DO NOTHING;
""")

artist_table_insert = ("""
    insert into artists (artist_id, name, location, lattitude, longitude)
    values(%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING;
""")


time_table_insert = ("""
    insert into time (start_time, hour, day, week, month, year, weekday)
    values(%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING;
""")

# FIND SONGS

song_select = ("""
    select s.song_id, s.artist_id
    from songs s
    join artists a on s.artist_id = a.artist_id
    where s.title = %s and a.name = %s and s.duration = %s
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
