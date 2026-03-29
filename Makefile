.PHONY: setup build up down restart logs logs-scheduler logs-webserver ps shell trigger-bootstrap trigger-core trigger-backfill clean

setup:
	bash setup.sh

build:
	docker compose build

up:
	docker compose up -d

down:
	docker compose down

restart:
	docker compose restart

logs:
	docker compose logs -f --tail=100

logs-scheduler:
	docker compose logs -f --tail=100 airflow-scheduler

logs-webserver:
	docker compose logs -f --tail=100 airflow-webserver

ps:
	docker compose ps

shell:
	docker exec -it chess_airflow_scheduler bash

trigger-bootstrap:
	docker exec chess_airflow_scheduler airflow dags trigger bootstrap_legacy_raw_state

trigger-core:
	docker exec chess_airflow_scheduler airflow dags trigger titled_players_daily
	docker exec chess_airflow_scheduler airflow dags trigger player_archives_incremental_daily
	docker exec chess_airflow_scheduler airflow dags trigger player_games_current_month_daily

trigger-backfill:
	docker exec chess_airflow_scheduler airflow dags trigger player_games_backfill

clean:
	docker compose down -v
