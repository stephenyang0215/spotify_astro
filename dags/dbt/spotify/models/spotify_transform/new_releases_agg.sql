SELECT artists_id FROM {{source('spotify', 'new_releases_album_tracks')}}