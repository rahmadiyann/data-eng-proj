
run-webserver:
	docker compose up webserver

run:
	docker compose down && docker compose up -d && sleep 10 && make run-webserver

stop:
	docker compose down