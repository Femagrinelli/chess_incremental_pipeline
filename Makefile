.PHONY: setup build up down restart logs logs-scheduler logs-webserver ps shell trigger-bootstrap trigger-core trigger-backfill trigger-silver trigger-silver-backfill trigger-gold trigger-gold-backfill duckdb-list duckdb-query duckdb-file duckdb-ui-up duckdb-ui-logs duckdb-ui-down clean

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

trigger-silver:
	docker exec chess_airflow_scheduler airflow dags trigger silver_title_roster_daily
	docker exec chess_airflow_scheduler airflow dags trigger silver_games_current_month_daily

trigger-gold:
	docker exec chess_airflow_scheduler airflow dags trigger gold_title_month_current_daily

trigger-silver-backfill:
ifdef SILVER_CONF
	docker exec chess_airflow_scheduler airflow dags trigger silver_games_month_backfill --conf '$(SILVER_CONF)'
else ifeq ($(MONTH_KEY),)
	docker exec chess_airflow_scheduler airflow dags trigger silver_games_month_backfill
else
	docker exec chess_airflow_scheduler airflow dags trigger silver_games_month_backfill --conf '{"month_key":"$(MONTH_KEY)"}'
endif

trigger-gold-backfill:
ifdef GOLD_CONF
	docker exec chess_airflow_scheduler airflow dags trigger gold_title_month_backfill --conf '$(GOLD_CONF)'
else ifeq ($(MONTH_KEY),)
	docker exec chess_airflow_scheduler airflow dags trigger gold_title_month_backfill
else
	docker exec chess_airflow_scheduler airflow dags trigger gold_title_month_backfill --conf '{"month_key":"$(MONTH_KEY)"}'
endif

duckdb-list:
	ls -1 sql

duckdb-query:
ifndef SQL
	$(error Use: make duckdb-query SQL="SELECT ...")
endif
	docker exec -i chess_airflow_scheduler python /opt/airflow/scripts/duckdb_query.py --sql "$(SQL)"

duckdb-file:
ifndef SQL_FILE
	$(error Use: make duckdb-file SQL_FILE=player_month_sample.sql)
endif
	docker exec -i chess_airflow_scheduler python /opt/airflow/scripts/duckdb_query.py --file /opt/airflow/sql/$(SQL_FILE)

duckdb-ui-up:
	docker compose --profile tools up -d --build duckdb-ui

duckdb-ui-logs:
	docker compose --profile tools logs -f --tail=100 duckdb-ui

duckdb-ui-down:
	docker compose --profile tools stop duckdb-ui

clean:
	docker compose down -v
