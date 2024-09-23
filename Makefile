# Makefile

.PHONY: initialize run-producer-consumer

initialize:
	docker-compose up -d broker schema-registry connect control-center ksqldb-server ksqldb-cli rest-proxy postgres flask

run-prodcon:
	docker-compose up -d producer consumer-album consumer-artist consumer-song consumer-history

build-prodcon:
	docker-compose up -d producer consumer-album consumer-artist consumer-song consumer-history --build