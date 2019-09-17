import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

ARN             = config.get('IAM_ROLE', 'ARN')
LOG_DATA        = config.get('S3', 'LOG_DATA')
LOG_JSONPATH    = config.get('S3', 'LOG_JSONPATH')
SONG_DATA       = config.get('S3', 'SONG_DATA')

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
        artist         VARCHAR NULL,
        auth           VARCHAR NULL,
        firstName      VARCHAR NULL,
        gender         VARCHAR NULL,
        itemInSession  VARCHAR NULL,
        lastName       VARCHAR NULL,
        length         TEXT NULL,
        level          VARCHAR NULL,
        location       VARCHAR NULL,
        method         VARCHAR NULL,
        page           VARCHAR NULL,
        registration   VARCHAR NULL,
        sessionId      INTEGER NOT NULL,
        song           VARCHAR NULL,
        status         INTEGER NULL,
        ts             BIGINT NOT NULL,
        userAgent      TEXT NULL,
        userId         INTEGER NULL
    )
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        song_id          VARCHAR(18) NOT NULL,
        num_songs        INTEGER NOT NULL,
        title            VARCHAR NOT NULL,
        artist_name      VARCHAR NOT NULL,
        artist_latitude  VARCHAR NULL,
        year             INTEGER NULL,
        duration         DECIMAL NOT NULL,
        artist_id        VARCHAR(18) NOT NULL,
        artist_longitude VARCHAR NULL,
        artist_location  VARCHAR NULL
    )
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id  INTEGER IDENTITY(0,1) NOT NULL SORTKEY,
        start_time   TIMESTAMP NOT NULL,
        user_id      INTEGER NOT NULL DISTKEY,
        level        VARCHAR(4) NOT NULL,
        song_id      VARCHAR(18) NOT NULL,
        artist_id    VARCHAR(18) NOT NULL,
        session_id   INTEGER NOT NULL,
        location     VARCHAR(50) NOT NULL,
        user_agent   VARCHAR(150) NOT NULL
    )
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id    INTEGER NOT NULL SORTKEY,
        first_name VARCHAR(25) NOT NULL,
        last_name  VARCHAR(25) NOT NULL,
        gender     VARCHAR(1) NOT NULL,
        level      VARCHAR(4) NOT NULL
    )
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id   VARCHAR NOT NULL SORTKEY,
        title     VARCHAR NOT NULL,
        artist_id VARCHAR NOT NULL,
        year      INTEGER NOT NULL,
        duration  DECIMAL NOT NULL
    )
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id   VARCHAR NULL SORTKEY,
        name        VARCHAR NULL,
        location    VARCHAR NULL,
        lattitude   DECIMAL NULL,
        longitude   DECIMAL NULL
    )
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time   TIMESTAMP NOT NULL SORTKEY,
        hour         INTEGER NOT NULL,
        day          INTEGER NOT NULL,
        week         INTEGER NOT NULL,
        month        INTEGER NOT NULL,
        year         INTEGER NOT NULL,
        weekday      INTEGER NOT NULL
    )
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events FROM {}
    credentials 'aws_iam_role={}'
    format as json {}
    region 'us-west-2';
""").format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("""
    COPY staging_songs FROM {}
    credentials 'aws_iam_role={}'
    format as json 'auto'
    region 'us-west-2';
""").format(SONG_DATA, ARN)

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (
        start_time,
        user_id,
        level,
        song_id,
        artist_id,
        session_id,
        location,
        user_agent
    )
    SELECT DISTINCT
        timestamp 'epoch' + (se.ts/1000) * interval '1 second' as timestamp,
        se.userid,
        se.level,
        ss.song_id,
        ss.artist_id,
        se.sessionid,
        ss.artist_location,
        se.useragent
    FROM
        staging_events se
    JOIN
        staging_songs ss on se.song = ss.title and se.artist = ss.artist_name
""")

user_table_insert = ("""
    INSERT INTO users (
        user_id,
        first_name,
        last_name,
        gender,
        level
    )
    SELECT DISTINCT
        userid,
        firstname,
        lastname,
        gender,
        level
        
    FROM
        staging_events
    WHERE
        userid is not null
""")

song_table_insert = ("""
    INSERT INTO songs (
        song_id,
        title,
        artist_id,
        year,
        duration
    )
    SELECT DISTINCT
        song_id,
        title,
        artist_id,
        year,
        duration
    FROM
        staging_songs
""")

artist_table_insert = ("""
    INSERT INTO artists (
        artist_id,
        lattitude,
        location,
        longitude,
        name
    )
    select distinct artist_id, artist_latitude, artist_location, artist_longitude, artist_name
	from staging_songs
""")

time_table_insert = ("""
    INSERT INTO time (
        start_time,
        hour,
        day,
        week,
        month,
        year,
        weekday 
    )
    SELECT DISTINCT
        timestamp 'epoch' + (ts/1000) * interval '1 second' as timestamp,
        EXTRACT(hour FROM timestamp) as hour,
        EXTRACT(day FROM timestamp) as day,
        EXTRACT(week FROM timestamp) as week,
        EXTRACT(month FROM timestamp) as month,
        EXTRACT(year FROM timestamp) as year,
        EXTRACT(weekday FROM timestamp) as weekday
    FROM
        staging_events
        
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
