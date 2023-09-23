import sqlalchemy
import sqlite3
from sqlalchemy.orm import sessionmaker
import pandas as pd
import requests
import json
import datetime as dt
from datetime import datetime, timedelta
from dotenv import load_dotenv
from requests import post, get
import os
import base64
from urllib.parse import urlencode

load_dotenv()

#Client credentials, redirect uri, authorization scope
client_id = os.getenv("CLIENT_ID")
client_secret = os.getenv("CLIENT_SECRET")
redirect_uri = 'http://localhost:3000/callback'
scope = "user-read-recently-played"

#DB Location
load = "sqlite:///user_tracks.sqlite"
engine = sqlalchemy.create_engine(load)
connect = sqlite3.connect("user_tracks.sqlite")
cursor = connect.cursor()

#Spotify Accounts service endpoints
auth_url = 'https://accounts.spotify.com/authorize'
token_url = 'https://accounts.spotify.com/api/token'

#Generate the authorization URL
def generate_authorization_url():
    params = {
        'client_id': client_id,
        'response_type': 'code',
        'redirect_uri': redirect_uri,
        'scope': scope,
    }
    return auth_url + '?' + urlencode(params)

#Request an access token using the authorization code
def request_access_token(authorization_code):
    headers = {
        'Authorization': 'Basic ' + base64.b64encode(f'{client_id}:{client_secret}'.encode()).decode(),
    }
    data = {
        'grant_type': 'authorization_code',
        'code': authorization_code,
        'redirect_uri': redirect_uri,
    }
    response = requests.post(token_url, headers=headers, data=data)
    return response.json()

#Extract user's recently played tracks
def get_recently_played_tracks(access_token):
    url = 'https://api.spotify.com/v1/me/player/recently-played'
    
    headers = {
        'Authorization': f'Bearer {access_token}',
    }
    now_unix_ms = int(dt.datetime.now().timestamp() * 1000)
    twenty_four_hours_ago_unix_ms = now_unix_ms - (24 * 60 * 60 * 1000)
    params = {
        'limit': 50, 
        'after': twenty_four_hours_ago_unix_ms,
    }
    response = requests.get(url, headers=headers, params=params)
    return response.json()

def validate_data(df: pd.DataFrame) -> bool:
    #Primary Key assignment
    if pd.Series(df['played_at']).is_unique:
        pass
    else: 
        raise Exception['Primary Key Check violated']
    
    if df.isnull().values.any():
        raise Exception("Null value found")
    
#Main function to initiate the Authorization Code flow
def main():
    auth_url = generate_authorization_url()
    print(f"Please visit the following URL to authorize the app: {auth_url}")
    
    authorization_code = input("Enter the authorization code from the URL: ")
    token_data = request_access_token(authorization_code)
    if 'access_token' in token_data:
        access_token = token_data['access_token']
        recently_played_tracks = get_recently_played_tracks(access_token)
        
        if 'items' in recently_played_tracks:
            print("Recently Played Tracks:")
            
            #Initialize empty lists to store data for DataFrame
            track_names = []
            artist_names = []
            played_at_list = []

            for track in recently_played_tracks['items']:
                track_name = track['track']['name']
                artist_name = track['track']['artists'][0]['name']
                played_at = track['played_at']

                #Append data to their respective lists
                track_names.append(track_name)
                artist_names.append(artist_name)
                played_at_list.append(played_at)

            #Create a pandas DataFrame from the lists
            song_df = pd.DataFrame({
                "track_name": track_names,
                "artist_name": artist_names,
                "played_at": played_at_list
            })

            print(song_df)
        else:
            print("No recently played tracks found.")
    else:
        print("Failed to obtain access token.")

    if validate_data(song_df):
        print("The data extracted is valid.")
    
    query = """CREATE TABLE IF NOT EXISTS user_tracks(
                track_name VARCHAR(200),
                artist_name VARCHAR(200),
                played_at VARCHAR(200),
                CONSTRAINT primary_key_constraint PRIMARY KEY(played_at)
    )"""
    
    cursor.execute(query)

    try:
        song_df.to_sql("user_tracks", engine, index=False, if_exists='append')
    except:
        print("Data failed to upload")

    connect.close()



if __name__ == '__main__':
    main()
