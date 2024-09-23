FROM python:3.9-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    sudo \
    curl \
    vim \
    unzip \
    rsync \
    build-essential \
    software-properties-common \
    ssh && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

## Download spark and hadoop dependencies and install

WORKDIR /app

COPY . .

RUN pip3 install --no-cache-dir -r requirements_producer.txt

CMD ["python3", "producer.py"]