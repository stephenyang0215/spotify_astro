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

    def search_for_artist(self, artist_name):
        url = 'https://api.spotify.com/v1/search'
        headers = self.get_auth_header()
        query = f'?q={artist_name}&type=artist&limit=5'
        query_url = url+query
        result = get(query_url, headers=headers)
        json_result = json.loads(result.content)['artists']['items']
        if len(json_result)==0:
            print('No artist with this name exists.')
            return None
        return json_result[0]
    def get_user_saved_track(self):
        url = f'https://api.spotify.com/v1/me/tracks'#?market=US&limit=5&offset=5'
        #url = 'https://api.spotify.com/v1/search'
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

    def get_songs_by_artist(self, artist_id):
        #top-tracks
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
    '''
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
 '''   
if __name__ == "__main__":
    extract = Extract()
    result = extract.search_for_artist('ACDC')
    spotify_pd = extract.get_songs_by_artist(result['id'])
    print(spotify_pd)
    #print('\n')
    #spotify_user_saved_track = extract.get_user_saved_track()
    #print(spotify_user_saved_track)