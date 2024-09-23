from flask import Flask, request, jsonify, redirect, session as flask_session, url_for
from spotipy import SpotifyOAuth
import uuid
import requests
import os
import secrets
import time
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models import Tokens, Base
from dotenv import load_dotenv
import logging

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

client_id = os.getenv('SPOTIFY_CLIENT_ID')
client_secret = os.getenv('SPOTIFY_CLIENT_SECRET')
TOKEN_INFO = ''

app = Flask(__name__)
app.secret_key = secrets.token_hex(16)
engine = create_engine(os.getenv('PSQL_LINK'))
Session = sessionmaker(bind=engine)
session = Session()
Base().metadata.create_all(engine)


def create_spotify_oauth():
    return SpotifyOAuth(
        client_id=client_id,
        client_secret=client_secret,
        redirect_uri=url_for('redirect_page', _external=True),
        scope='user-read-recently-played',
    )

def get_token():
    token_info = session.query(Tokens).filter_by(id=1).first()
    if not token_info.access_token or not token_info.refresh_token or not token_info.expires_at:
        logger.error("No token info found")
        return redirect(url_for('login', _external=True))
    
    is_expired = token_info.expires_at - int(time.time()) < 600
    
    if is_expired:
        logger.info("Token expired, refreshing token")
        sp_oauth = create_spotify_oauth()
        token_info = sp_oauth.refresh_access_token(token_info.refresh_token)
        save_token_info(token_info)
        
    return {
        'access_token': token_info.access_token,
        'refresh_token': token_info.refresh_token,
        'expires_at': token_info.expires_at
    }

def save_token_info(token_info):
    tokens = session.query(Tokens).filter_by(id=1).first()
    if not tokens:
        tokens = Tokens(id=1)
        tokens.access_token = token_info['access_token']
        tokens.refresh_token = token_info['refresh_token']
        tokens.expires_at = token_info['expires_at']
        session.add(tokens)
    else:
        tokens.access_token = token_info['access_token']
        tokens.refresh_token = token_info['refresh_token']
        tokens.expires_at = token_info['expires_at']
    session.commit()
    

@app.route('/', methods=['GET'])
def login():
    auth_url = create_spotify_oauth().get_authorize_url()
    return redirect(auth_url)

@app.route('/save', methods=['POST'])
def save():
    token_info = get_token()['access_token']
    url = 'https://api.spotify.com/v1/me/player/recently-played'
    header = {
        'Authorization': f'Bearer {token_info}',
    }
    
    req_data = request.get_json()
    after = req_data.get('after', round(time.time() * 1000))
    recent_played = requests.get(url, headers=header, params={'after': after, 'limit': 50}).json()
    if recent_played.get('items', []) == []:
        return {
            "status_code": 204,
            "message": "No new data"
        }
        
    items = []
    for item in recent_played['items']:
        played_at = item['played_at'],
        song_id = item['track']['id']
        album_id = item['track']['album']['id']
        artist_id = item['track']['artists'][0]['id']
        items.append({
            'id': uuid.uuid4().hex,
            'played_at': played_at[0],
            'song_id': song_id,
            'album_id': album_id,
            'artist_id': artist_id,
        })
        
    return jsonify({
        'status_code': 200,
        'message': f'{len(items)} new songs',
        'items': items,
    })

@app.route('/callback')
def redirect_page():
    flask_session.clear()
    code = request.args.get('code')
    token_info = create_spotify_oauth().get_access_token(code)
    save_token_info(token_info)
    flask_session[TOKEN_INFO] = token_info
    # return redirect(url_for('save', _external=True))
    return jsonify({'status_code': 200, 'message': 'Successfully logged in. Try making post request to "/save"'})  # Ensure 'homepage' is defined correctly
    
@app.route('/print', methods=['GET'])
def print_env():
    return jsonify({
        "client_id": client_id,
        "client_secret": client_secret,
        "psql_link": os.getenv('PSQL_LINK')
    })
    

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000)