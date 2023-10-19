from Spotify_Token import Auth_Token
import json
import pandas as pd
import requests
from requests import get
import os 
import snowflake.connector
from Load import load_snoflake_conn, verify_internal_stage


class Extract(Auth_Token):
    def __init__(self):
        super().__init__()
        self.snowflake_user=os.getenv('snowflake_user')
        self.snowflake_password=os.getenv('snowflake_password')
        self.snowflake_account=os.getenv('snowflake_account')
        self.snowflake_db=os.getenv('snowflake_db')
        self.snowflake_schema=os.getenv('snowflake_schema')
        self.snowflake_warehouse=os.getenv('snowflake_warehouse')

    def search_for_artist(self, artist_name):
        url = 'https://api.spotify.com/v1/search'
        headers = self.get_auth_header()
        query = f'?q={artist_name}&type=track%2Cartist&limit=5'
        query_url = url+query
        result = get(query_url, headers=headers)
        json_result = json.loads(result.content)
        if len(json_result)==0:
            print('No artist with this name exists.')
            return None
        json_artists = json_result['artists']['items'][0]
        json_tracks = json_result['tracks']['items'][0]
        return {'artist_id':json_artists['id'], 'artist_genres':json_artists['genres'], 'track_id':json_tracks['id']}
    
    def get_songs_by_artist(self, artist_id):
        album_lst = []
        album_id_lst = []
        album_type_lst = []
        song_lst = []
        #https://developer.spotify.com/documentation/web-api/reference/get-an-artists-top-tracks
        url = f'https://api.spotify.com/v1/artists/{artist_id}/top-tracks?country=US'
        try:
            headers = self.get_auth_header()
            result = get(url, headers=headers)
            result.raise_for_status()
        except requests.exceptions.RequestException as e:
            raise SystemExit(e)
        
        json_result = json.loads(result.content)['tracks']
        for track in json_result:
            album_lst.append(track['album']['name'])
            album_id_lst.append(track['album']['id'])
            album_type_lst.append(track['album']['album_type'])
            song_lst.append(track['name'])
        pd_result = pd.DataFrame({'album': album_lst, 'album_id':album_id_lst, 'album_type':album_type_lst, 'track': song_lst})
        return pd_result
    
    def get_track_by_album(self, album_id):
        #https://developer.spotify.com/documentation/web-api/reference/get-an-albums-tracks
        artists_album_lst = ['href', 'id', 'name', 'type', 'uri']
        dict_artists = dict()
        for col in artists_album_lst:
            dict_artists[col] = []
        track_album_lst = ['href', 'id', 'name', 'type', 'uri']
        dict_tracks = dict()
        for col in track_album_lst:
            dict_tracks[col] = []
        url = f'https://api.spotify.com/v1/albums/{album_id}/tracks?market=US'
        headers = self.get_auth_header()
        result = get(url, headers=headers)
        json_result = json.loads(result.content)['items']
        for track in json_result:
            for col in artists_album_lst:
                dict_artists[col].append(track['artists'][0][col])
            for col in track_album_lst:
                dict_tracks[col].append(track[col])
        track_artist = pd.DataFrame.from_dict(dict_artists)
        artists_album_lst = ['artists_'+col if 'artists' not in col else col for col in artists_album_lst]
        track_artist.columns = artists_album_lst
        track_album = pd.DataFrame.from_dict(dict_tracks)
        track_album_lst = ['track_'+col if 'track' not in col else col for col in track_album_lst]
        track_album.columns = track_album_lst
        result = pd.concat([track_artist, track_album], axis=1)
        result = result.loc[:,~result.columns.duplicated()].copy()
        result['album_id'] = album_id
        return result
    
    def get_recommendation(self, artist_id, genres, track_id):
        track_album_lst = ['album_type', 'total_tracks', 'available_markets', 'href', 'id', 'name', 'release_date', 'release_date_precision', 'type', 'uri']
        dict_album = dict()
        for col in track_album_lst:
            dict_album[col] = []
        track_artist_lst = [ 'href', 'id', 'name', 'type', 'uri']
        dict_artist = dict()
        for col in track_artist_lst:
            dict_artist[col] = []
        url = f'https://api.spotify.com/v1/recommendations?seed_artists={artist_id}&seed_genres={genres}&seed_tracks={track_id}'
        headers = self.get_auth_header()
        result = get(url, headers=headers)
        json_result = json.loads(result.content)['tracks']
        if len(json_result)==0:
            print('No recommendation exists.')
            return None

        for track in json_result:
            for col in track_album_lst:
                dict_album[col].append(track['album'][col])
            for col in track_artist_lst:
                dict_artist[col].append(track['artists'][0][col])
        album_result = pd.DataFrame.from_dict(dict_album)
        track_album_lst = ['album_'+col if 'album' not in col else col for col in track_album_lst]
        album_result.columns = track_album_lst
        artist_result = pd.DataFrame.from_dict(dict_artist)
        track_artist_lst = ['artist_'+col if 'artist' not in col else col for col in track_artist_lst]
        artist_result.columns = track_artist_lst
        result = pd.concat([album_result, artist_result], axis=1)
        result = result.loc[:,~result.columns.duplicated()].copy()
        return result
    
    def get_new_releases(self):
        #Get a list of new album releases featured in Spotify
        #https://developer.spotify.com/documentation/web-api/reference/get-new-releases
        releases_album_lst = ['album_type', 'total_tracks', 'available_markets', 'href', 'id', 'name', 'release_date', 'release_date_precision', 'type', 'uri']
        releases_artists_lst = ['href', 'id', 'name', 'type', 'uri']
        dict_album = dict()
        dict_artist = dict()
        for col in releases_album_lst:
            dict_album[col] = []
        for col in releases_artists_lst:
            dict_artist[col] = []
        url = 'https://api.spotify.com/v1/browse/new-releases?country=US&limit=30'
        headers = self.get_auth_header()
        result = get(url, headers=headers)
        json_result = json.loads(result.content)
        if len(json_result)==0:
            print('No release exists.')
            return None
        for album in json_result['albums']['items']:
            for col in releases_album_lst:
                dict_album[col].append(album[col])
            for col in releases_artists_lst:
                dict_artist[col].append(album['artists'][0][col])
        album_result = pd.DataFrame.from_dict(dict_album)
        releases_album_lst = ['album_'+col if 'album' not in col else col for col in releases_album_lst]
        album_result.columns = releases_album_lst
        artist_result = pd.DataFrame.from_dict(dict_artist)
        releases_artists_lst = ['album_'+col if 'album' not in col else col for col in releases_artists_lst]
        artist_result.columns = releases_artists_lst
        result = pd.concat([album_result, artist_result], axis=1)
        result = result.loc[:,~result.columns.duplicated()].copy()
        return result
    
    def export_id_list(self, feature, schema, table_name):
        id_lst = []
        ctx = snowflake.connector.connect(
            user=self.snowflake_user,
            password=self.snowflake_password,
            account=self.snowflake_account,
            warehouse='compute_wh',
            role='accountadmin',
            database='spotify',
            schema =schema
            )
        cs = ctx.cursor()
        #Test connection to snowflake
        sql = """SELECT 1"""
        cs.execute(sql)
        first_row = cs.fetchone()
        assert first_row[0] == 1
        #fetch the dataset  
        sql = f"""SELECT DISTINCT {feature} FROM SPOTIFY.{schema}.{table_name}_FLATTEN"""
        cs.execute(sql)
        cs_tb = cs.fetchall()
        for row in cs_tb:
            id_lst.append(row[0])
        return id_lst
    
    def get_category_playlists(self, category_id):
        col_lst = ['description', 'href', 'id', 
            'name', 'public', 'snapshot_id', 'type', 'uri']
        dict_category_playlist = dict()
        for col in col_lst:
            dict_category_playlist[col] = []
        url = f'https://api.spotify.com/v1/browse/categories/{category_id}/playlists?country=US&limit=50'
        headers = self.get_auth_header()
        result = get(url, headers=headers)
        json_result = json.loads(result.content)
        if len(json_result)==0:
            print('No playlist exists.')
            return None
        for playlist in json_result['playlists']['items']:
            if type(playlist) == dict:
                for col in col_lst:
                    dict_category_playlist[col].append(playlist[col])
        category_playlists = pd.DataFrame.from_dict(dict_category_playlist)
        return category_playlists

    def get_featured_playlists(self):
        playlist_track_lst = ['description', 'id', 'name', 'public', 'total', 'uri']
        dict_playlist = dict()
        for col in playlist_track_lst:
            dict_playlist[col] = []
        url = 'https://api.spotify.com/v1/browse/featured-playlists?country=US&limit=50'
        headers = self.get_auth_header()
        result = get(url, headers=headers)
        json_result = json.loads(result.content)
        if len(json_result)==0:
            print('No playlist exists.')
            return None
        for playlist in json_result['playlists']['items']:
            for col in playlist_track_lst:
                if col == 'total':
                    dict_playlist['total'].append(playlist['tracks']['total'])
                else:
                    dict_playlist[col].append(playlist[col])
        featured_playlists = pd.DataFrame.from_dict(dict_playlist)
        return featured_playlists
    
    def get_playlist(self, playlist_id):
        album_lst = ['album_type', 'total_tracks', 'available_markets', 'id', 'name', 'release_date', 'type', 'uri']
        album_dict = {}
        for col in album_lst:
            album_dict[col] = []
        artist_lst = ['id', 'name', 'uri']
        artist_dict = {}
        for col in artist_lst:
            artist_dict[col] = []
        track_lst = ['id', 'name', 'popularity', 'uri']
        track_dict = {}
        for col in track_lst:
            track_dict[col] = []
        url = f'https://api.spotify.com/v1/playlists/{playlist_id}'
        headers = self.get_auth_header()
        result = get(url, headers=headers)
        json_result = json.loads(result.content)
        if len(json_result)==0:
            print('No playlist exists.')
            return None
        for track in json_result['tracks']['items']:
            for col in album_lst:
                album_dict[col].append(track['track']['album'][col])
            for col in artist_lst:
                artist_dict[col].append(track['track']['artists'][0][col])
            for col in track_lst:
                track_dict[col].append(track['track'][col])
        album_df = pd.DataFrame.from_dict(album_dict)
        artist_df = pd.DataFrame.from_dict(artist_dict)
        track_df = pd.DataFrame.from_dict(track_dict)
        album_df.columns = ['album_'+col if 'album' not in col else col for col in album_lst]
        artist_df.columns = ['artist_'+col if 'artist' not in col else col for col in artist_lst]
        track_df.columns = ['track_'+col if 'track' not in col else col for col in track_lst]
        result = pd.concat([album_df, artist_df, track_df], axis=1)
        result = result.loc[:,~result.columns.duplicated()].copy()
        result['total'] = json_result['tracks']['total']
        return result
    
    def extract_spotify_json_file(self, url, table_name):
        headers = self.get_auth_header()
        result = get(url, headers=headers)
        json_result = json.loads(result.content)
        result = json.dumps(json_result)
        #write to json file
        try:
            with open(f'files/{table_name}.json', 'w') as fp2:
                    fp2.write(result)
            print(os.path.abspath(f'files/{table_name}.json'))
        except Exception as e:
            print('Error: ' + str(e))
        print('Successfully write to the json file!')
        return list(json_result.keys())
   
if __name__ == "__main__":
    extract = Extract()
    result = extract.get_category_playlists('0JQ5DAqbMKFQ00XGBls6ym')

    '''
    def get_user_saved_track(self):
        url = f'https://api.spotify.com/v1/me/tracks'
        try:
            headers = self.get_auth_header()
            result = get(url, headers=headers)
            result.raise_for_status()
        except requests.exceptions.RequestException as e:
            raise SystemExit(e)
        
        json_track = json.loads(result.content)['items']['track']
        for track in json_track:
            #album of the track
            self.album_type = json_track['album']['album_type']
            self.album_total_tracks = json_track['album']['total_tracks']
            self.album_id = json_track['album']['id']
            self.album_name = json_track['album']['name']
            self.album_release_date = json_track['album']['release_date']
            #artist of the track
            self.artist_genre = json_track['artists']['genres']
            self.artist_id = json_track['artists']['id']
            self.artist_name = json_track['artists']['name']
            self.artist_popularity = json_track['artists']['popularity']
            self.artist_uri = json_track['artists']['uri']
            #track
            self.track_id = json_track['id']
            self.track_name = json_track['name']
            self.track_popularity = json_track['popularity']
            self.track_number = json_track['track_number']
            self.track_uri = json_track['uri']
        pd_result = pd.DataFrame({'album_type':self.album_type , 'album_total_tracks':self.album_total_tracks, 'album_id':self.album_id,
                                  'album_name': self.album_name, 'album_release_date':self.album_release_date, 
                                  'artist_genre':self.artist_genre, 'artist_id': self.artist_id, 'artist_name':self.artist_name,
                                  'artist_popularity':self.artist_popularity, 'artist_uri':self.artist_uri,
                                  'track_id':self.track_id, 'track_name':self.track_name, 'track_popularity':self.track_popularity,
                                  'track_number':self.track_number, 'track_uri':self.track_uri})
        return pd_result
        '''