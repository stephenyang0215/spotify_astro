from Spotify_Token import Auth_Token
import json
import pandas as pd
import requests
from requests import get
import os 

class Extract(Auth_Token):
    def __init__(self):
        super().__init__()
        self.album = []
        self.album_tracks = []
        self.album_type = []
        self.album_id = []
        self.song = []

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
    def search_for_artist(self, artist_name):
        #https://developer.spotify.com/documentation/web-api/reference/search
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
        #return json_result['artists']['items'][0], json_result['tracks']['items'][0]
    def get_songs_by_artist(self, artist_id):
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
            self.album.append(track['album']['name'])
            self.album_id.append(track['album']['id'])
            self.album_type.append(track['album']['album_type'])
            self.song.append(track['name'])
        pd_result = pd.DataFrame({'album': self.album, 'album_id':self.album_id, 'album_type':self.album_type, 'track': self.song})
        return pd_result
    
    def get_track_by_album(self, spotify_pd):
        for album_id in spotify_pd['album'].unique():
            album_name = spotify_pd[spotify_pd['album_id']==album_id]['album'].tolist()[0]
            album_tracks = []
            url = f'https://api.spotify.com/v1/albums/{album_id}/tracks?market=US'
            headers = self.get_auth_header()
            result = get(url, headers=headers)
            json_result = json.loads(result.content)['items']
            for track in json_result:
                album_tracks.append(track['name'])
        return album_tracks
    
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
   
if __name__ == "__main__":
    extract = Extract()
    artist_track = extract.search_for_artist('ACDC')
    spotify_pd = extract.get_songs_by_artist(artist_track['artist_id'])
    recommendation = extract.get_recommendation(artist_track['artist_id'], artist_track['artist_genres'], artist_track['track_id'])
    print(recommendation.columns)