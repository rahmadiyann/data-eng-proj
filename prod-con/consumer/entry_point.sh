#!/bin/bash

case "$ROLE" in
    "consumer-album")
        python3 spark-consumer-album.py
        ;;
    "consumer-artist")
        python3 spark-consumer-artist.py
        ;;
    "consumer-song")
        python3 spark-consumer-song.py
        ;;
    "consumer-history")
        python3 spark-consumer-history.py
        ;;
    *)
        echo "Unknown script: $ROLE"
        exit 1
        ;;
esac