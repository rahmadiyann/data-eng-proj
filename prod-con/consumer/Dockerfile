FROM python:3.9-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    sudo \
    curl \
    vim \
    unzip \
    rsync \
    openjdk-17-jdk \
    build-essential \
    software-properties-common \
    ssh && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

## Download spark and hadoop dependencies and install
RUN wget -O /opt/spark-3.5.2-bin-hadoop3.tgz https://www.apache.org/dyn/closer.lua/spark/spark-3.5.2/spark-3.5.2-bin-hadoop3.tgz && \
    tar -xzf /opt/spark-3.5.2-bin-hadoop3.tgz -C /opt/ && \
    mv /opt/spark-3.5.2 /opt/spark

# Optional env variables
ENV SPARK_HOME=${SPARK_HOME:-"/opt/spark"}

RUN mkdir -p ${SPARK_HOME}

COPY spark /opt/spark

RUN chmod u+x /opt/spark/bin/* && \
    chmod u+x /opt/spark/sbin/*


ENV SPARK_HOME="/opt/spark"

WORKDIR /app

COPY . .

RUN pip3 install --no-cache-dir -r requirements.txt

COPY entry_point.sh /app/entry_point.sh
RUN chmod +x /app/entry_point.sh

ENTRYPOINT ["/app/entry_point.sh"]