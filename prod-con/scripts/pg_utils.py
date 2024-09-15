from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models import Base, DimAlbum, DimArtist, DimSong, FactHistory
from spotipy_utils import fetch_artist_data, fetch_album_data, fetch_song_data
from dotenv import load_dotenv
import os

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '..', '..', '.env'))
psql_link = os.getenv('PSQL_LINK')

engine = create_engine(psql_link)
Session = sessionmaker(bind=engine)

# Create tables if they do not exist
Base.metadata.create_all(engine)

def insert_listening_history(listening_data):
    session = Session()
    try:
        # Check if song exists
        song = session.query(DimSong).filter_by(id=listening_data['song_id']).first()
        if not song:
            song_data = fetch_song_data(listening_data['song_id'])
            song = DimSong(**song_data)
            session.add(song)
        
        # Check if album exists
        album = session.query(DimAlbum).filter_by(id=listening_data['album_id']).first()
        if not album:
            album_data = fetch_album_data(listening_data['album_id'])
            album = DimAlbum(**album_data)
            session.add(album)
        
        # Check if artist exists
        artist = session.query(DimArtist).filter_by(id=listening_data['artist_id']).first()
        if not artist:
            artist_data = fetch_artist_data(listening_data['artist_id'])
            artist = DimArtist(**artist_data)
            session.add(artist)
        
        # Insert listening history
        listening = FactHistory(**listening_data)
        session.add(listening)
        
        session.commit()
    except Exception as e:
        session.rollback()
        raise e
    finally:
        session.close()
     
listened_at = datetime.now()
song_id = "4UWRWtfJN9Um7jfjCOsaKN"
album_id = "5MeQb1GnEnO31Hz0jkTPc5"
artist_id = "2oKg78fkes6YHMRk4I8aAa"
insert_listening_history({
    'listened_at': listened_at,
    'song_id': song_id,
    'album_id': album_id,
    'artist_id': artist_id
})