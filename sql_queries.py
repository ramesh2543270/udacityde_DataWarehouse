import configparser
from Manage_RS_Cluster import get_Cluster_Props

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# Get role ARN
_ , DWH_ARN = get_Cluster_Props()

# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS event_stg"
staging_songs_table_drop = "DROP TABLE IF EXISTS song_stg"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS event_stg
    (
        artist          VARCHAR,
        auth            VARCHAR, 
        firstName       VARCHAR,
        gender          VARCHAR,   
        itemInSession   INTEGER,
        lastName        VARCHAR,
        length          FLOAT,
        level           VARCHAR, 
        location        VARCHAR,
        method          VARCHAR,
        page            VARCHAR,
        registration    BIGINT,
        sessionId       INTEGER,
        song            VARCHAR,
        status          INTEGER,
        ts              bigint,
        userAgent       VARCHAR,
        userId          INTEGER
    );
""")


staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS song_stg
    (
        song_id             VARCHAR,
        title               VARCHAR,
        duration            FLOAT,
        year                INTEGER,
        artist_id           VARCHAR,
        artist_name          VARCHAR,
        artist_latitude      REAL,
        artist_longitude     REAL,
        artist_location      VARCHAR,
        num_songs            INTEGER
    );
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays
    (
        songplayId    BIGINT IDENTITY(1, 1) PRIMARY KEY,
        startTime     TIMESTAMP NOT NULL SORTKEY,
        userid         INTEGER NOT NULL DISTKEY,
        level          VARCHAR,
        songId        VARCHAR,
        artistId      VARCHAR,
        sessionId     INTEGER,
        location       VARCHAR,
        userAgent     VARCHAR
    ) diststyle key;
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users
    (
        userid      INTEGER PRIMARY KEY SORTKEY,
        firstName   VARCHAR,
        lastName    VARCHAR,
        gender      VARCHAR,
        level       VARCHAR
    ) diststyle all;
""")

song_table_create = ("""
     CREATE TABLE IF NOT EXISTS songs
    (
        songId      VARCHAR PRIMARY KEY SORTKEY,
        title       VARCHAR,
        artistId   VARCHAR DISTKEY,
        year        INTEGER,
        duration    FLOAT
    ) diststyle key;
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists
    (
        artistId    VARCHAR PRIMARY KEY SORTKEY,
        name        VARCHAR,
        location    VARCHAR,
        latitude    REAL,
        longitude   REAL
    ) diststyle all;
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time
    (
        startTime  TIMESTAMP PRIMARY KEY SORTKEY,
        hour        SMALLINT,
        day         SMALLINT,
        week        SMALLINT,
        month       SMALLINT,
        year        SMALLINT DISTKEY,
        weekday     SMALLINT
    ) diststyle key;
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY {} FROM {}
    IAM_ROLE '{}'
    JSON {} region '{}';
    """).format(
        'event_stg',
        config.get("S3","LOG_DATA"),
        DWH_ARN,
        config.get("S3","LOG_JSONPATH"),
        'us-west-2'
    )

staging_songs_copy = ("""
    COPY {} FROM {}
    IAM_ROLE '{}'
    JSON 'auto' region '{}';
    """).format(
        'song_stg',
        config.get("S3","SONG_DATA"),
        DWH_ARN,
        'us-west-2'
)

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (startTime, userId, level, songId, artistId, sessionId, location, userAgent) 
        SELECT
            TIMESTAMP 'epoch' + (e.ts/1000 * INTERVAL '1 second'),
            e.userId,
            e.level,
            s.song_id,
            s.artist_id,
            e.sessionId,
            e.location,
            e.userAgent
        FROM event_stg e
        JOIN song_stg s ON
            e.song = s.title AND
            e.artist = s.artist_name 
        WHERE
            e.page = 'NextSong'
""")


user_table_insert = ("""
    INSERT INTO users 
        SELECT  DISTINCT (userId)
                userId,
                firstName,
                lastName,
                gender,
                level
        FROM event_stg
        WHERE userid is not null
""")

song_table_insert = ("""
    INSERT INTO songs 
        SELECT DISTINCT (song_id)
            artist_id,
            title,
            artist_id,
            year,
            duration
        FROM song_stg
""")

artist_table_insert = ("""
    INSERT INTO artists (artistId,name,location,latitude,longitude)
      SELECT DISTINCT (artist_id)
        artist_id,
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude
    FROM song_stg
""")

time_table_insert = ("""
    INSERT INTO time 
        WITH temp_ts AS (SELECT TIMESTAMP 'epoch' + (ts/1000 * INTERVAL '1 second') as ts FROM event_stg)
        SELECT DISTINCT
            ts,
            extract(hour from ts),
            extract(day from ts),
            extract(week from ts),
            extract(month from ts),
            extract(year from ts),
            extract(weekday from ts)
        FROM temp_ts
""")

# QUERY LISTS
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
