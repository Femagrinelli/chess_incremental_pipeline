.PHONY: setup build up down restart logs logs-scheduler logs-webserver ps shell trigger-bootstrap trigger-core trigger-backfill trigger-bronze trigger-bronze-backfill trigger-dbt-silver trigger-dbt-silver-backfill trigger-dbt-gold trigger-dbt-gold-backfill dbt-debug dbt-ls clean

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

trigger-bronze:
	docker exec chess_airflow_scheduler airflow dags trigger bronze_roster_daily
	docker exec chess_airflow_scheduler airflow dags trigger bronze_games_current_month_daily

trigger-dbt-silver:
	docker exec chess_airflow_scheduler airflow dags trigger dbt_silver_current_daily

trigger-dbt-gold:
	docker exec chess_airflow_scheduler airflow dags trigger dbt_gold_current_daily

trigger-bronze-backfill:
ifdef BRONZE_CONF
	docker exec chess_airflow_scheduler airflow dags trigger bronze_games_month_backfill --conf '$(BRONZE_CONF)'
else ifeq ($(MONTH_KEY),)
	docker exec chess_airflow_scheduler airflow dags trigger bronze_games_month_backfill
else
	docker exec chess_airflow_scheduler airflow dags trigger bronze_games_month_backfill --conf '{"month_key":"$(MONTH_KEY)"}'
endif

trigger-dbt-silver-backfill:
ifdef DBT_SILVER_CONF
	docker exec chess_airflow_scheduler airflow dags trigger dbt_silver_backfill --conf '$(DBT_SILVER_CONF)'
else ifeq ($(MONTH_KEY),)
	docker exec chess_airflow_scheduler airflow dags trigger dbt_silver_backfill
else
	docker exec chess_airflow_scheduler airflow dags trigger dbt_silver_backfill --conf '{"month_key":"$(MONTH_KEY)"}'
endif

trigger-dbt-gold-backfill:
ifdef DBT_GOLD_CONF
	docker exec chess_airflow_scheduler airflow dags trigger dbt_gold_backfill --conf '$(DBT_GOLD_CONF)'
else ifeq ($(MONTH_KEY),)
	docker exec chess_airflow_scheduler airflow dags trigger dbt_gold_backfill
else
	docker exec chess_airflow_scheduler airflow dags trigger dbt_gold_backfill --conf '{"month_key":"$(MONTH_KEY)"}'
endif

dbt-debug:
	docker exec chess_airflow_scheduler dbt debug --project-dir /opt/airflow/dbt/chess_medallion --profiles-dir /opt/airflow/dbt/profiles

dbt-ls:
	docker exec chess_airflow_scheduler dbt ls --project-dir /opt/airflow/dbt/chess_medallion --profiles-dir /opt/airflow/dbt/profiles

clean:
	docker compose down -v
