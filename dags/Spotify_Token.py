from dotenv import load_dotenv
import base64
import json
import os
from requests import post
class Auth_Token():
    def __init__(self):
        #self.base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
        load_dotenv()
        #os.path.join(self.base_dir, '.env'))
        self.client_id = os.getenv('client_id')
        self.client_secret = os.getenv('client_secret')

    def get_token(self):
        auth_string = self.client_id + ":" +self.client_secret
        auth_bytes = auth_string.encode('utf-8')
        auth_base64 = str(base64.b64encode(auth_bytes), 'utf-8')

        url = 'https://accounts.spotify.com/api/token'
        headers = {
            'Authorization': 'Basic ' + auth_base64,
            'Content-Type': 'application/x-www-form-urlencoded'
        }
        data = {'grant_type':'client_credentials'}#, 'scope': 'user-library-read user-read-email'}
        result = post(url, headers=headers, data=data)
        json_result = json.loads(result.content)
        token = json_result['access_token']
        return token

    def get_auth_header(self):
        token = self.get_token()
        return {'Authorization': 'Bearer ' + token}

if __name__ == '__main__':
    Auth_Token()
