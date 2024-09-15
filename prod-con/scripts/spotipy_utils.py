import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from dotenv import load_dotenv
import os
import json

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '..', '..', '.env'))

client_id = os.getenv('ETL_CLIENT_ID')
client_secret = os.getenv('ETL_CLIENT_SECRET')

sp = spotipy.Spotify(client_credentials_manager=SpotifyClientCredentials(client_id=client_id, client_secret=client_secret))

def fetch_artist_data(artist_id: str) -> dict:
    response = sp.artist(artist_id)
    data = {
        "id": artist_id,
        "name": response['name'],
        "external_url": response['external_urls']['spotify'],
        "follower_count": response['followers']['total'],
        "image_url": response['images'][0]['url'],
        "popularity": response['popularity'],
    }
    return data

def fetch_album_data(album_id: str) -> dict:
    response = sp.album(album_id)
    data = {
        "id": album_id,
        "title": response['name'],
        "total_tracks": response['total_tracks'],
        "release_date": response['release_date'],
        "external_url": response['external_urls']['spotify'],
        "image_url": response['images'][0]['url'],
        "label": response['label'],
        "popularity": response['popularity'],
    }
    return data

def fetch_song_data(song_id: str) -> dict:
    response = sp.track(song_id)
    data = {
        "id": song_id,
        "title": response['name'],
        "disc_number": response['disc_number'],
        "duration_ms": response['duration_ms'],
        "explicit": response['explicit'],
        "external_url": response['external_urls']['spotify'],
        "preview_url": response['preview_url'],
        "popularity": response['popularity'],
    }
    return data

print(json.dumps(fetch_artist_data("2oKg78fkes6YHMRk4I8aAa"), indent=2))
