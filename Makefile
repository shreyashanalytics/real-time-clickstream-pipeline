.PHONY: up down reset logs topic sql test lint

up:
	docker compose up --build -d

down:
	docker compose down

reset:  ## wipe containers AND all stored data
	docker compose down -v

logs:
	docker compose logs -f stream-processor

topic:  ## peek at 5 raw events in Kafka
	docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
	  --bootstrap-server kafka:29092 --topic clickstream.events --from-beginning --max-messages 5

sql:  ## open a ClickHouse SQL prompt
	docker compose exec clickhouse clickhouse-client --user app --password app_pw --database clickstream

test:
	pytest -q

lint:
	ruff check . && ruff format --check .
