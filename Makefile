run:
	docker compose down && docker compose up

run-d:
	docker compose down && docker compose up -d

run-webserver-d:
	docker compose up webserver -d

run-webserver:
	docker compose up webserver

stop:
	docker compose down