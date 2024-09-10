FROM apache/airflow:latest

USER root
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get clean

# Set JAVA_HOME environment variable
ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-amd64

USER airflow

RUN pip install apache-airflow apache-airflow-providers-openlineage>=1.8.0 apache-airflow-providers-apache-spark pyspark
