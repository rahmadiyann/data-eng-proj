import json
from flask import Flask, redirect, session, request, url_for
from spotipy import Spotify, SpotifyOAuth
import os
from dotenv import load_dotenv
import secrets
import time

load_dotenv()

client_id = os.getenv('ETL_CLIENT_ID')
client_secret = os.getenv('ETL_CLIENT_SECRET')
TOKEN_INFO = ''
app = Flask(__name__)
app.secret_key = secrets.token_hex(16)

def create_spotify_oauth():
    return SpotifyOAuth(
        client_id=client_id,
        client_secret=client_secret,
        redirect_uri=url_for('redirect_page', _external=True),
        scope='user-read-recently-played'
    )
    
def get_token():
    token_info = load_token_info()
    if not token_info:
        return redirect(url_for('login', _external=True))
        
    is_expired = token_info['expires_at'] - int(time.time()) < 600
    if is_expired:
        sp_oauth = create_spotify_oauth()
        token_info = sp_oauth.refresh_access_token(token_info['refresh_token'])
        save_token_info(token_info)
    return token_info['access_token']

def save_token_info(token_info):
    # save token info to a file
    with open('token_info.json', 'w') as f:
        f.write(json.dumps(token_info))

def load_token_info(): 
    try:
        with open('token_info.json', 'r') as file:
            data = json.load(file)
        return data
    except FileNotFoundError:
        return None
    except KeyError:
        return None

@app.route('/', methods=['GET'])
def homepage():
    try:
        token_info = get_token()
    except:
        print('User not logged in / token expired')
        return redirect(url_for('login', _external=True))
    sp = Spotify(auth=token_info)
    
    # Get 'after' from the POST request, if not provided, set default to 7 days ago
    # after = request.form.get('after')
    recent_played = sp.current_user_recently_played(limit=2)
    items = []
    for item in recent_played['items']:
        played_at = item['played_at'],
        song_id = item['track']['id']
        song_name = item['track']['name']
        song_duration_ms = item['track']['duration_ms']
        popularity = item['track']['popularity']
        song_link = item['track']['external_urls']['spotify']
        song_uri = item['track']['uri']
        explicit = item['track']['explicit']
        album_id = item['track']['album']['id']
        album_name = item['track']['album']['name']
        album_link = item['track']['album']['external_urls']['spotify']
        album_uri = item['track']['album']['uri']
        album_release_date = item['track']['album']['release_date']
        album_total_tracks = item['track']['album']['total_tracks']
        artist_id = item['track']['artists'][0]['id']
        artist_name = item['track']['artists'][0]['name']
        artist_link = item['track']['artists'][0]['external_urls']['spotify']
        artist_uri = item['track']['artists'][0]['uri']
        items.append({
            'played_at': played_at[0],
            'song_id': song_id,
            'song_name': song_name,
            'song_duration_ms': song_duration_ms,
            'popularity': popularity,
            'song_link': song_link,
            'song_uri': song_uri,
            'explicit': explicit,
            'album_id': album_id,
            'album_name': album_name,
            'album_link': album_link,
            'album_uri': album_uri,
            'album_release_date': album_release_date,
            'album_total_tracks': album_total_tracks,
            'artist_id': artist_id,
            'artist_name': artist_name,
            'artist_link': artist_link,
            'artist_uri': artist_uri
        })
        
    data = {
        # we're fething recent_played['cursors']['before'] as the parameter for the next api call so we're not getting the same data
        'after': recent_played['cursors']['before'],
        'items': items
    }
    return data
    
    
@app.route('/login', methods=['GET'])
def login():
    auth_url = create_spotify_oauth().get_authorize_url()
    return redirect(auth_url)

@app.route('/redirect')
def redirect_page():
    session.clear()
    code = request.args.get('code')
    token_info = create_spotify_oauth().get_access_token(code)
    save_token_info(token_info)
    session[TOKEN_INFO] = token_info
    return redirect(url_for('homepage', _external=True),)