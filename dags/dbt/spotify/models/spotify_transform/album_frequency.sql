{{
    config(
        materialized='table'
    )
}}
SELECT ALBUM, COUNT(*) frequency FROM {{source('spotify', 'get_songs_by_artist')}}
GROUP BY ALBUM ORDER BY frequency DESC