with open('../files/browse_categories_playlists.sql', 'r') as browse_categories, \
        open('../files/featured_playlists_albums_artists_tracks.sql', 'r') as featured_playlists, \
        open('../files/new_releases_album_tracks.sql', 'r') as new_releases: \

    browse_categories_sql = browse_categories.read()
    featured_playlists_sql = featured_playlists.read()
    new_releases_sql = new_releases.read()

print(browse_categories_sql)
import os
print(os.listdir())

