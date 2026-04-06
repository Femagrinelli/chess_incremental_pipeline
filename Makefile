.PHONY: setup build up down restart logs logs-scheduler logs-webserver ps shell duckdb-shell duckdb-file trigger-daily trigger-bronze trigger-silver trigger-gold trigger-backfill trigger-bootstrap streamlit-up streamlit-logs streamlit-down dbt-debug dbt-ls clean

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

duckdb-shell:
	docker exec -it chess_airflow_scheduler python /opt/airflow/scripts/duckdb_sql_shell.py

duckdb-file:
ifndef SQL_FILE
	$(error SQL_FILE is required, for example SQL_FILE=sql/player_games.sql)
endif
	docker exec -i chess_airflow_scheduler python /opt/airflow/scripts/duckdb_sql_file.py $(if $(DUCKDB_FORMAT),--format $(DUCKDB_FORMAT),) $(if $(DUCKDB_MAX_ROWS),--max-rows $(DUCKDB_MAX_ROWS),) $(if $(DUCKDB_MAX_COL_WIDTH),--max-col-width $(DUCKDB_MAX_COL_WIDTH),) /opt/airflow/$(SQL_FILE)

trigger-daily:
	docker exec chess_airflow_scheduler airflow dags trigger chess_daily

trigger-bronze:
ifdef CONF
	docker exec chess_airflow_scheduler airflow dags trigger chess_bronze --conf '$(CONF)'
else ifdef MONTH_KEY
	docker exec chess_airflow_scheduler airflow dags trigger chess_bronze --conf '{"month_key":"$(MONTH_KEY)"}'
else
	docker exec chess_airflow_scheduler airflow dags trigger chess_bronze
endif

trigger-silver:
ifdef CONF
	docker exec chess_airflow_scheduler airflow dags trigger chess_silver --conf '$(CONF)'
else ifdef MONTH_KEY
	docker exec chess_airflow_scheduler airflow dags trigger chess_silver --conf '{"month_key":"$(MONTH_KEY)"}'
else
	docker exec chess_airflow_scheduler airflow dags trigger chess_silver
endif

trigger-gold:
ifdef CONF
	docker exec chess_airflow_scheduler airflow dags trigger chess_gold --conf '$(CONF)'
else ifdef MONTH_KEY
	docker exec chess_airflow_scheduler airflow dags trigger chess_gold --conf '{"month_key":"$(MONTH_KEY)"}'
else
	docker exec chess_airflow_scheduler airflow dags trigger chess_gold
endif

trigger-backfill:
	docker exec chess_airflow_scheduler airflow dags trigger chess_backfill

trigger-bootstrap:
	docker exec chess_airflow_scheduler airflow dags trigger chess_bootstrap

streamlit-up:
	docker compose --profile tools up -d --build --remove-orphans gold-dashboard

streamlit-logs:
	docker compose --profile tools logs -f --tail=100 gold-dashboard

streamlit-down:
	docker compose --profile tools stop gold-dashboard

dbt-debug:
	docker exec chess_airflow_scheduler dbt debug --project-dir /opt/airflow/dbt/chess_medallion --profiles-dir /opt/airflow/dbt/profiles

dbt-ls:
	docker exec chess_airflow_scheduler dbt ls --project-dir /opt/airflow/dbt/chess_medallion --profiles-dir /opt/airflow/dbt/profiles

clean:
	docker compose down -v
