import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

ARN = config.get('IAM_ROLE','ARN')
LOG_DATA = config.get('S3','LOG_DATA')
SONG_DATA = config.get('S3','SONG_DATA')
LOG_JSONPATH = config.get('S3','LOG_JSONPATH')


# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events_table"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs_table"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events_table (
        artist varchar,
        auth varchar,
        FirstName varchar,
        gender varchar,
        itemInSession int,
        lastName varchar,
        length float, 
        level varchar,
        location varchar,
        method varchar,
        page varchar,
        registration float,
        sessionId int,
        song varchar,
        status int,
        ts bigint,
        userAgent varchar,
        userId int);
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs_table (
        num_songs int,
        artist_id varchar,
        artist_latitude float,
        artist_longitude float, 
        artist_location varchar,
        artist_name varchar, 
        song_id varchar, 
        title varchar,
        duration float,
        year int);
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id bigint identity(0, 1) primary key,
        start_time timestamp not null,
        user_id int not null,
        level varchar not null,
        song_id varchar not null,
        artist_id varchar not null,
        session_id int not null,
        location varchar,
        user_agent varchar);
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id int primary key,
        first_name varchar,
        last_name varchar,
        gender varchar,
        level varchar);
""")



song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id varchar primary key,
        title varchar not null,
        artist_id varchar not null,
        year int,
        duration float);
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id varchar primary key,
        name varchar not null,
        location varchar,
        latitude float,
        longitude float);
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        time_id bigint identity(0, 1) primary key,
        start_time timestamp not null,
        hour int,
        day int,
        week int,
        month int,
        year int,
        weekday int);
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events_table
    from '{}'
    credentials 'aws_iam_role={}'
    FORMAT JSON AS
    '{}'
    region 'us-west-2';
""").format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("""
     copy staging_songs_table
     from '{}'
     credentials 'aws_iam_role={}'
     FORMAT AS JSON 
     'auto'
     region 'us-west-2';
""").format(SONG_DATA, ARN)

# FINAL TABLES

songplay_table_insert =  ("""
    INSERT INTO songplays
        (
            start_time,
            user_id,
            level,
            song_id,
            artist_id,
            session_id,
            location,
            user_agent
        )
    SELECT timestamp 'epoch' + e.ts * interval '0.001 seconds',
        e.userId,
        e.level,
        s.song_id,
        s.artist_id,
        e.sessionId,
        e.location,
        e.userAgent
    FROM staging_events_table e
    JOIN staging_songs_table s
    ON e.song = s.title
    WHERE e.page='NextSong';
""")

user_table_insert = ("""
    INSERT INTO users
        (
            user_id,
            first_name,
            last_name,
            gender,
            level
        )  
    SELECT DISTINCT userId,
        FirstName,
        lastName,
        gender,
        level
    FROM staging_events_table
    WHERE page='NextSong';
""")

song_table_insert = ("""
    INSERT INTO songs
        (
            song_id,
            title,
            artist_id,
            year,
            duration
        )
    SELECT DISTINCT song_id,
        title,
        artist_id,
        year,
        duration
    FROM staging_songs_table;
""")

artist_table_insert = ("""
    INSERT into artists
        (
            artist_id,
            name,
            location,
            latitude,
            longitude
        )
    SELECT DISTINCT artist_id,
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude
    FROM staging_songs_table;
""")

time_table_insert = ("""
    INSERT INTO time
        (
            start_time,
            hour,
            day,
            week,
            month,
            year,
            weekday
        )
    SELECT 
        timestamp 'epoch' + ts * interval '0.001 seconds',
        extract(hour from timestamp 'epoch' + ts * interval '0.001 seconds'),
        extract(day from timestamp 'epoch' + ts * interval '0.001 seconds'),
        extract(week from timestamp 'epoch' + ts * interval '0.001 seconds'),
        extract(month from timestamp 'epoch' + ts * interval '0.001 seconds'),
        extract(year from timestamp 'epoch' + ts * interval '0.001 seconds'),
        extract(weekday from timestamp 'epoch' + ts * interval '0.001 seconds')
    FROM staging_events_table;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
