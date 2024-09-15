from sqlalchemy import Column, Integer, String, ForeignKey, DateTime, BigInteger, Boolean
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class DimArtist(Base):
    __tablename__ = 'dim_artist'
    id = Column(String, primary_key=True)
    name = Column(String, nullable=False)
    external_url = Column(String, nullable=False)
    follower_count = Column(Integer, nullable=False)
    image_url = Column(String, nullable=False)
    popularity = Column(Integer, nullable=False)

class DimAlbum(Base):
    __tablename__ = 'dim_album'
    id = Column(String, primary_key=True)
    title = Column(String, nullable=False)
    total_tracks = Column(Integer, nullable=False)
    release_date=Column(DateTime, nullable=False)
    external_url = Column(String, nullable=False)
    image_url = Column(String, nullable=False)
    label = Column(String, nullable=False)
    popularity = Column(Integer, nullable=False)

class DimSong(Base):
    __tablename__ = 'dim_song'
    id = Column(String, primary_key=True)
    title = Column(String, nullable=False)
    disc_number = Column(Integer, nullable=False)
    duration_ms = Column(BigInteger, nullable=False)
    explicit = Column(Boolean, nullable=False)
    external_url = Column(String, nullable=False)
    preview_url = Column(String, nullable=False)
    popularity = Column(Integer, nullable=False)

class FactHistory(Base):
    __tablename__ = 'fact_history'
    id = Column(Integer, primary_key=True, autoincrement=True)
    listened_at = Column(DateTime, nullable=False)
    song_id = Column(String, ForeignKey('dim_song.id'), nullable=False)
    album_id = Column(String, ForeignKey('dim_album.id'), nullable=False)
    artist_id = Column(String, ForeignKey('dim_artist.id'), nullable=False)
    