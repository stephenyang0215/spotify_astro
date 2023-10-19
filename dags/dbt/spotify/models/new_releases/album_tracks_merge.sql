{{ 
    config(
        materialized='table'
    ) 
}}

select * from 
    (select DISTINCT albums_items_artists_id as artists_id, albums_items_artists_name as artists_name,
    albums_items_id as albums_id, albums_items_name as albums_name, albums_items_release_date as release_date
    from {{ source('spotify', 'new_releases_flatten')}}) album
join 
    (select DISTINCT track_href, track_id, track_name, track_uri, album_id 
    from {{ source('spotify', 'new_releases_album_tracks')}}) track 
on album.albums_items_id = track.album_id