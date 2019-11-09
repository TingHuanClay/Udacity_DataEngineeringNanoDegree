import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES
## staging Table
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
## Fact Table
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
## Dimension Tables
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"


# CREATE TABLES (based on AWS Redshift)

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events(
        artist              VARCHAR,
        auth                VARCHAR,
        firstName           VARCHAR,
        gender              VARCHAR,
        itemInSession       INTEGER,
        lastName            VARCHAR,
        length              FLOAT,
        level               VARCHAR,
        location            VARCHAR,
        method              VARCHAR,
        page                VARCHAR,
        registration        FLOAT,
        sessionId           INTEGER,
        song                VARCHAR,
        status              INTEGER,
        ts                  TIMESTAMP,
        userAgent           VARCHAR,
        userId              INTEGER 
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs(
        num_songs           INTEGER,
        artist_id           VARCHAR,
        artist_latitude     FLOAT,
        artist_longitude    FLOAT,
        artist_location     VARCHAR,
        artist_name         VARCHAR,
        song_id             VARCHAR,
        title               VARCHAR,
        duration            FLOAT,
        year                INTEGER
    );
""")

# SERIAL in Postgres is NOT supported in Redshift and replaced with IDENTITY
songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays
    (
        songplay_id INTEGER     IDENTITY(0,1)   PRIMARY KEY,
        start_time  TIMESTAMP   NOT NULL SORTKEY DISTKEY,
        user_id     VARCHAR     NOT NULL,
        level       VARCHAR,
        song_id     VARCHAR     NOT NULL,
        artist_id   VARCHAR     NOT NULL,
        session_id  VARCHAR, 
        location    VARCHAR, 
        user_agent  VARCHAR
    );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users
    (
        user_id     VARCHAR     PRIMARY KEY SORTKEY,
        first_name  VARCHAR     NOT NULL, 
        last_name   VARCHAR     NOT NULL, 
        gender      VARCHAR, 
        level       VARCHAR
    );
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs
    (
        song_id     VARCHAR     PRIMARY KEY SORTKEY, 
        title       VARCHAR     NOT NULL, 
        artist_id   VARCHAR     NOT NULL, 
        year        INTEGER, 
        duration    FLOAT
    );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists
    (
        artist_id   VARCHAR     PRIMARY KEY SORTKEY, 
        name        VARCHAR     NOT NULL, 
        location    VARCHAR, 
        latitude    FLOAT, 
        longitude   FLOAT
    );
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time
    (
        start_time  TIMESTAMP   PRIMARY KEY DISTKEY SORTKEY, 
        hour        INTEGER, 
        day         INTEGER,
        week        INTEGER,
        month       INTEGER,
        year        INTEGER,
        weekday     VARCHAR
    );
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events from {}
    region 'us-west-2'
    iam_role '{}'
    compupdate off statupdate off
    format as json {}
    timeformat as 'epochmillisecs'
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
    copy staging_songs from {data_bucket}
    credentials 'aws_iam_role={role_arn}'
    region 'us-west-2'
    format as JSON 'auto';
""").format(data_bucket=config['S3']['SONG_DATA'], role_arn=config['IAM_ROLE']['ARN'])

# FINAL TABLES (Fact table and dimension tables)

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT DISTINCT(e.ts)  AS start_time, 
           e.userId        AS user_id, 
           e.level         AS level, 
           s.song_id, 
           s.artist_id, 
           e.sessionId     AS session_id, 
           e.location, 
           e.userAgent     AS user_agent
      FROM staging_events e
      JOIN staging_songs  s ON (e.song = s.title AND e.artist = s.artist_name)
       AND e.page = 'NextSong'
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT(userId)    AS user_id,
           firstName           AS first_name,
           lastName            AS last_name,
           gender,
           level
      FROM staging_events
     WHERE user_id IS NOT NULL
       AND page  =  'NextSong';
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT DISTINCT(song_id) AS song_id,
           title,
           artist_id,
           year,
           duration
      FROM staging_songs
     WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT(artist_id) AS artist_id,
           artist_name         AS name,
           artist_location     AS location,
           artist_latitude     AS latitude,
           artist_longitude    AS longitude
      FROM staging_songs
     WHERE artist_id IS NOT NULL;
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT(start_time)                AS start_time,
           EXTRACT(hour FROM start_time)       AS hour,
           EXTRACT(day FROM start_time)        AS day,
           EXTRACT(week FROM start_time)       AS week,
           EXTRACT(month FROM start_time)      AS month,
           EXTRACT(year FROM start_time)       AS year,
           EXTRACT(dayofweek FROM start_time)  as weekday
      FROM songplays;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
copy_table_titles = ['copying staging_events table', 'copying staging_songs table']
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
insert_table_titles = ["inserting songplays table", "inserting users table", "inserting songs table", "inserting artists table", "inserting time table"]


# Basic Analytical Query (only query the total data count)
# For further ETL operation
# We could change the count to the process count
# which would change the ETL to progressive but not update whole table
count_staging_events_table = ("""
    SELECT COUNT(*) AS total FROM staging_events
""")

count_staging_songs_table = ("""
    SELECT COUNT(*) AS total FROM staging_songs
""")

count_songplay_table = ("""
    SELECT COUNT(*) AS total FROM songplays
""")

count_user_table = ("""
    SELECT COUNT(*) AS total FROM users
""")

count_song_table = ("""
    SELECT COUNT(*) AS total FROM songs
""")

count_artist_table = ("""
    SELECT COUNT(*) AS total FROM artists
""")

count_time_table = ("""
    SELECT COUNT(*) AS total FROM time
""")

data_count_queries = [
    count_staging_events_table, count_staging_songs_table,
    count_songplay_table,
    count_user_table, count_song_table, count_artist_table, count_time_table
]

data_count_queries_titles = [
    'count_staging_events_table', 'count_staging_songs_table',
    'count_songplay_table',
    'count_user_table', 'count_song_table', 'count_artist_table', 'count_time_table'
]
