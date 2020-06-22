users_query = ("""
SELECT  DISTINCT
    userId    AS user_id,
    firstName AS first_name,
    lastName  AS last_name,
    gender,
    level
FROM events
WHERE userId IS NOT NULL
AND page  =  'NextSong'
""")

songs_table_query = ("""
SELECT DISTINCT 
    song_id, 
    title, 
    artist_id, 
    year, 
    duration 
FROM songs 
WHERE song_id IS NOT NULL
""")

artists_table_query = ("""
SELECT DISTINCT
    artist_id,
    artist_name         AS name,
    artist_location     AS location,
    artist_latitude     AS latitude,
    artist_longitude    AS longitude
FROM songs
WHERE artist_id IS NOT NULL

""")

log_filtered_query = ("""
SELECT 
    *,
    cast(ts/1000 as Timestamp) as timestamp   
FROM events 
WHERE page = 'NextSong'
""")


time_query = ("""
SELECT DISTINCT
    ts,
    timestamp                          AS start_time,
    EXTRACT(hour FROM timestamp)       AS hour,
    EXTRACT(day FROM timestamp)        AS day,
    EXTRACT(week FROM timestamp)       AS week,
    EXTRACT(month FROM timestamp)      AS month,
    EXTRACT(year FROM timestamp)       AS year,
    EXTRACT(dayofweek FROM timestamp)  AS weekday
FROM events

""")

songplays_query = ("""
SELECT DISTINCT
    (e.ts)          AS start_time, 
    e.userId        AS user_id, 
    e.level         AS level, 
    s.song_id       AS song_id, 
    s.artist_id     AS artist_id, 
    e.sessionId     AS session_id, 
    e.location      AS location, 
    e.userAgent     AS user_agent,
    EXTRACT(month FROM e.timestamp)      AS month,
    EXTRACT(year FROM e.timestamp)       AS year  
FROM events e
JOIN songs  s   ON (e.song = s.title)
AND e.page  =  'NextSong'
""")