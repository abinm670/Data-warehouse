import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

### IAM amazon User Account
KEY=config.get('AWS','key')
SECRET= config.get('AWS','secret')

### Cluster Info
DB_NAME= config.get("CLUSTER","DB_NAME")
DB_USER= config.get("CLUSTER","DB_USER")
DB_PASSWORD= config.get("CLUSTER","DB_PASSWORD")
DB_PORT = config.get("CLUSTER","DB_PORT")
HOST=config.get("CLUSTER", "HOST")
### Role 
ARN="arn:aws:iam::910225714303:role/myRedshiftRole"

### Souerce of database
LOG_DATA=config.get("S3", "LOG_DATA")
LOG_JSONPATH=config.get("S3", "LOG_JSONPATH") 
SONG_DATA=config.get("S3", "SONG_DATA")  





# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop =  "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES 

### staging tables

staging_events_table_create= ("""

CREATE TABLE staging_events(
event_id INT IDENTITY(0,1),
artist VARCHAR,
auth VARCHAR,
first_name VARCHAR,
gender CHAR,
item_In_Session INT,
last_name VARCHAR,
length DOUBLE PRECISION,
level VARCHAR,
location text,
method VARCHAR,
page VARCHAR,
registration VARCHAR,
session_id BIGINT,
song_name VARCHAR,
status INTEGER,
ts timestamp,
user_agent TEXT,
user_id VARCHAR,
PRIMARY KEY(event_id))

""")

staging_songs_table_create = (""" CREATE TABLE staging_songs( 
            song_id VARCHAR,
            num_songs INT,
            artist_id VARCHAR,
            artist_latitude DOUBLE PRECISION,
            artist_longitude DOUBLE PRECISION,
            artist_location  VARCHAR,
            artist_name  VARCHAR,
            title TEXT,
            duration DOUBLE PRECISION,
            year INT,
            PRIMARY KEY (song_id))
""")


### fact tables
# songplay_table_create = ("""CREATE TABLE songplays(
#     songplay_id INT IDENTITY(0,1),
#     start_time timestamp,
#     user_id VARCHAR,
#     level VARCHAR,
#     song_id VARCHAR,
#     artist_id VARCHAR,
#     session_id BIGINT,
#     location TEXT,
#     user_agent TEXT,
#     PRIMARY KEY (songplay_id))
# """)


### dimensions
# user_table_create = ("""CREATE TABLE users(
# user_id VARCHAR,
# first_name VARCHAR, 
# last_name VARCHAR, 
# gender CHAR,
# level VARCHAR,
# PRIMARY KEY (user_id))
# """)

song_table_create = (""" CREATE TABLE songs(
    song_id VARCHAR,
    title TEXT, 
    artist_id VARCHAR, 
    year INT, 
    duration DOUBLE PRECISION,
    PRIMARY KEY (song_id))
""")

# artist_table_create = ("""CREATE TABLE artists(
# artist_id VARCHAR,
# artist_name VARCHAR, 
# artist_location text,
# artist_latitude DOUBLE PRECISION,
# artist_longitude DOUBLE PRECISION,
# PRIMARY KEY (artist_id))
# """)

# time_table_create = (""" CREATE TABLE time(
#     start_time timestamp,
#     hour INT, 
#     day INT,
#     week INT,
#     month INT,
#     year INT, PRIMARY KEY (start_time))  
# """)


# STAGING TABLES

staging_events_copy = (""" copy staging_events
            from 's3://udacity-dend/log_data'
            credentials 'aws_iam_role={}'
            JSON {}
""").format(ARN, config.get('S3', 'LOG_JSONPATH'))

# staging_songs_copy = (""" copy staging_songs 
#         from {}
#     credentials 'aws_iam_role={}'
#     json 'auto';
#  """).format(config.get('S3', 'SONG_DATA'), ARN)



# FINAL TABLES

# songplay_table_insert = ("""
# INSERT INTO songplays (start_time, user_id, level,  song_id, artist_id, session_id, location, user_agent)
#     select timestamp 'epoch' + event.ts/1000 * interval '1 second' start_time,
#     event.user_id,
#     event.level,
#     song.song_id,
#     song.artist_id,
#     event.session_id,
#     event.location,
#     event.user_agent
# FROM staging_events event, staging_songs  song
# WHERE event.page = 'NextSong'
# AND event.song_name = song.title
# AND event.length = song.duration

# """)


# user_table_insert = ("""
# INSERT INTO users (user_id,first_name,last_name,gender,level)
# SELECT DISTINCT user_id, first_name, last_name, gender, level
# FROM staging_events
# WHERE page = 'NextSong'
# WHERE user_id IS NOT NULL
# """)

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT DISTINCT 
song_id,title, artist_id, year, duration
FROM staging_songs
WHERE song_id IS NOT NULL
""")


#artist_table_insert =
# (""" INSERT INTO (artist_id, artist_name, artist_location, artist_latitude,artist_longitude) \
#      SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_latitude \
#          FROM staging_songs WHERE artist_id IS NOT NULL """)




# time_table_insert = ("""
# INSERT INTO time(start_time,hour, day, week,month, year )
# SELECT start_time, 
#     extract (hour from start_time), 
#     extract(day from start_time),
#     extract(week from start_time),
#     extract(week from start_time), 
#     extract(month from start_time),
#     extract(year from start_time)
# FROM songplays
# """)

# QUERY LISTS
create_table_queries = [staging_events_table_create,staging_songs_table_create]
#[staging_songs_table_create] 
#staging_events_table_create,,songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = staging_events_copy
'staging_songs_copy'
#staging_songs_copy
insert_table_queries =''

#songplay_table_insert
#song_table_insert
# [songplay_table_insert]
#, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
