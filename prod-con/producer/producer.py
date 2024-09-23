import requests
import time
from spotipy import Spotify
from spotipy.oauth2 import SpotifyClientCredentials
from confluent_kafka import Producer, KafkaException
from dotenv import load_dotenv
import os
import json
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models import Base, DimAlbum, DimArtist, DimSong
from sqlalchemy.exc import OperationalError
import logging

logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s')
logger = logging.getLogger(__name__)



def fetch_artist_data(artist_id: str) -> dict:
    try:
        response = sp.artist(artist_id)
    except requests.exceptions.ReadTimeout:
        logger.info(f"Timeout occurred for artist_id: {artist_id}. Retrying...")
        response = sp.artist(artist_id)
    data = {
        "artist_id": artist_id,
        "name": response['name'],
        "external_url": response['external_urls']['spotify'],
        "follower_count": response['followers']['total'],
        "image_url": response['images'][0]['url'],
        "popularity": response['popularity'],
    }
    return data

def fetch_album_data(album_id: str) -> dict:
    try:
        response = sp.album(album_id)
    except requests.exceptions.ReadTimeout:
        logger.info(f"Timeout occurred for album_id: {album_id}. Retrying...")
        response = sp.album(album_id)
    data = {
        "album_id": album_id,
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
    try:
        response = sp.track(song_id)
    except requests.exceptions.ReadTimeout:
        logger.info(f"Timeout occurred for song_id: {song_id}. Retrying...")
        response = sp.track(song_id)
    data = {
        "artist_id": song_id,
        "title": response['name'],
        "disc_number": response['disc_number'],
        "duration_ms": response['duration_ms'],
        "explicit": response['explicit'],
        "external_url": response['external_urls']['spotify'],
        "preview_url": response['preview_url'],
        "popularity": response['popularity'],
    }
    return data

def produce_to_kafka(data, topic):
    producer = Producer({'bootstrap.servers': bootstrap_server})

    def delivery_callback(err, msg):
        if err:
            logger.error(f'ERROR: Message failed delivery: {err}')
        else:
            key = msg.key().decode('utf-8') if msg.key() else None
            value = msg.value().decode('utf-8') if msg.value() else None
            logger.info(f"Produced event to topic {msg.topic()}: key = {key} value = {value}")

    try:
        producer.produce(topic, value=json.dumps(data).encode('utf-8'), callback=delivery_callback)
        producer.flush()
        logger.info(f"Produced data to topic {topic}\n")
    except KafkaException as e:
        logger.error(f"Error producing to Kafka: {e}")

if __name__ == '__main__':
    load_dotenv()
    
    #env variables
    bootstrap_server=os.getenv('KAFKA_BOOTSTRAP_SERVER')
    client_id = os.getenv('SPOTIFY_CLIENT_ID')
    client_secret = os.getenv('SPOTIFY_CLIENT_SECRET')
    
    #spotify initialization
    sp = Spotify(client_credentials_manager=SpotifyClientCredentials(client_id=client_id, client_secret=client_secret))
    
    #psql initialization
    psql_link = os.getenv('PSQL_LINK')
    engine = create_engine(psql_link)
    Session = sessionmaker(bind=engine)
    Base.metadata.create_all(engine)
    
    # Create a single session
    session = Session()
    
    #endpoint initialization
    endpoint_url = 'http://flask:8000/save'
    data = {
        'after': int(time.time() * 10000)
    }
    try:
        while True:
            try:
                response = requests.post(endpoint_url, json=data).json()
                if response['status_code'] == 204:
                    logger.info(response)
                elif response['status_code'] == 200:
                    for item in response['items']:
                        song_data = session.query(DimSong).filter_by(song_id=item['song_id']).first()
                        if not song_data:
                            produce_to_kafka(fetch_song_data(item['song_id']), 'song_topic')
                        album_data = session.query(DimAlbum).filter_by(album_id=item['album_id']).first()
                        if not album_data:
                            produce_to_kafka(fetch_album_data(item['album_id']), 'album_topic')
                        artist_data = session.query(DimArtist).filter_by(artist_id=item['artist_id']).first()
                        if not artist_data:
                            produce_to_kafka(fetch_artist_data(item['artist_id']), 'artist_topic')
                        item_data = {
                            'song_id': item['song_id'],
                            'album_id': item['album_id'],
                            'artist_id': item['artist_id'],
                            'played_at': item['played_at']
                        }
                        produce_to_kafka(item_data, 'item_topic')
            except OperationalError as e:
                logger.error(f"Database operation failed: {e}")
                session.rollback()

            last_fetched = round(time.time() * 10000) 
            time.sleep(60)
    except KeyboardInterrupt:
        logger.info("Process interrupted by user.")
    finally:
        session.close()
        engine.dispose()
        logger.info("All sessions closed and resources released.")