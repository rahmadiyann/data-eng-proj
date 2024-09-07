FROM quay.io/astronomer/astro-runtime:12.0.0

ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-amd64

RUN rm -rf /app/*
COPY /app/ /app

USER root
RUN chmod -R 777 /app