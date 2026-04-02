# Chess Incremental Data Platform

An Airflow + AWS S3 + dbt data engineering project for Chess.com titled-player ingestion.

The project keeps the legacy raw landing zone, adds a structured bronze parquet layer, and uses dbt with a medallion architecture for bronze, silver, and gold models in Athena.

## Architecture

```text
                           +----------------------+
                           |  Chess.com PubAPI    |
                           |----------------------|
                           | /titled/{title}      |
                           | /player/{u}/archives |
                           | /player/{u}/Y/M      |
                           +----------+-----------+
                                      |
                                      v
                    +---------------------------------------+
                    |         Airflow Ingestion DAGs        |
                    |---------------------------------------|
                    | titled snapshots                      |
                    | archive refresh                       |
                    | current-month games                   |
                    | historical backfill                   |
                    +----------+----------------------------+
                               |
                               v
                    +---------------------------------------+
                    |        S3 Raw + State Layer           |
                    |---------------------------------------|
                    | raw/titled_players                    |
                    | raw/player_archives                   |
                    | raw/player_games                      |
                    | state/chess_com/...                   |
                    +----------+----------------------------+
                               |
                               v
                    +---------------------------------------+
                    |       Airflow Bronze DAGs             |
                    |---------------------------------------|
                    | bronze_roster_daily                   |
                    | bronze_games_current_month_daily      |
                    | bronze_games_month_backfill           |
                    +----------+----------------------------+
                               |
                               v
                    +---------------------------------------+
                    |        S3 Bronze Parquet              |
                    |---------------------------------------|
                    | bronze/chess_com/roster_daily         |
                    | bronze/chess_com/games_core           |
                    | bronze/chess_com/player_game_facts    |
                    +----------+----------------------------+
                               |
                               v
                    +---------------------------------------+
                    |        dbt Medallion Models           |
                    |---------------------------------------|
                    | bronze views over Athena sources      |
                    | silver cleaned fact tables            |
                    | gold analytical marts                 |
                    +----------+----------------------------+
                               |
               +---------------+------------------+
               |                                  |
               v                                  v
   +-------------------------------+   +-------------------------------+
   | Athena / Glue Catalog         |   | S3 Silver / Gold Parquet      |
   |-------------------------------|   |-------------------------------|
   | chess_bronze_external         |   | silver/chess_com/...          |
   | chess_bronze                  |   | gold/chess_com/...            |
   | chess_silver                  |   +-------------------------------+
   | chess_gold                    |
   +-------------------------------+
```

## Medallion Layout

### Raw

The project keeps the legacy raw JSON layout:

```text
raw/
├── titled_players/{TITLE}/{YYYY-MM-DD}.json
├── player_archives/{username}/{YYYY-MM-DD}.json
├── player_archives/{username}/{YYYY}/{MM}.json
└── player_games/{username}/{YYYY}/{MM}.json
```

### State

The incremental control layer stays under:

```text
state/chess_com/
├── title_state/title={TITLE}/current.json
└── player_index/username_prefix={xx}/username={username}/index.json
```

### Bronze

Bronze is the first structured parquet layer. It is still close to the source, but flattened and deduplicated enough to support modeling.

```text
bronze/chess_com/
├── roster_daily/
│   └── snapshot_date=YYYY-MM-DD/title=GM/part-000.parquet
├── games_core/
│   └── year=YYYY/month=MM/bucket=00/part-000.parquet
└── player_game_facts/
    └── year=YYYY/month=MM/bucket=00/part-000.parquet
```

Datasets:

- `roster_daily`: one row per titled player per roster snapshot.
- `games_core`: one row per unique Chess.com game.
- `player_game_facts`: one row per player per game, including opponent, color, rating, and score.

### Silver

Silver is now strictly the cleaned, structured layer.

```text
silver/chess_com/
├── roster_daily/
├── games_core/
└── player_game_facts/
```

Datasets:

- `silver_roster_daily`: de-duplicated roster snapshots for downstream joins.
- `silver_games_core`: canonical game fact table, partitioned by year and month.
- `silver_player_game_facts`: cleaned titled-player-per-game fact table, partitioned by year and month.

### Gold

Gold is now the presentation layer for reusable analytics.

```text
gold/chess_com/
├── title_month_activity/
├── title_month_rating_volatility/
└── title_month_color_performance/
```

Datasets:

- `gold_title_month_activity`
- `gold_title_month_rating_volatility`
- `gold_title_month_color_performance`

## Why This Structure Works Better

- Raw remains a full historical landing zone and does not need to be re-downloaded.
- State keeps the daily Chess.com call volume low.
- Bronze is structured once from raw and reused everywhere else.
- Silver stays focused on cleaning, shaping, and canonical facts.
- Gold stays focused on business-ready aggregates.
- dbt owns the model layer and the medallion contracts instead of mixing transformations into Python DAG code.

## dbt Project Structure

```text
dbt/
├── profiles/
│   └── profiles.yml
└── chess_medallion/
    ├── dbt_project.yml
    ├── macros/
    │   ├── filters.sql
    │   └── register_bronze_external_tables.sql
    └── models/
        ├── bronze/
        ├── silver/
        └── gold/
```

Model responsibilities:

- `models/bronze`: dbt views over Athena external tables registered on top of the bronze parquet files.
- `models/silver`: cleaned and partitioned analytical fact tables.
- `models/gold`: title-month marts for dashboards and BI.

## Project Structure

```text
chess_incremental_pipeline/
├── airflow/
│   └── dags/
├── dbt/
│   ├── profiles/
│   └── chess_medallion/
├── scripts/
├── docker-compose.yml
├── Dockerfile
├── Makefile
├── requirements.txt
├── setup.sh
└── .env.example
```

## Prerequisites

- Docker and Docker Compose on the VPS
- AWS credentials with access to:
  - S3
  - Athena
  - Glue Data Catalog
- An existing S3 bucket for raw, state, bronze, silver, gold, and dbt staging

## Environment

Copy the example file and edit the real runtime values:

```bash
cp .env.example .env
```

Important variables:

- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `AWS_REGION`
- `S3_BUCKET`
- `RAW_PREFIX`
- `BRONZE_PREFIX`
- `STATE_PREFIX`
- `SILVER_PREFIX`
- `GOLD_PREFIX`
- `PARQUET_BUCKET_COUNT`
- `DBT_ATHENA_CATALOG`
- `DBT_ATHENA_SOURCE_SCHEMA`
- `DBT_ATHENA_BRONZE_SCHEMA`
- `DBT_ATHENA_SILVER_SCHEMA`
- `DBT_ATHENA_GOLD_SCHEMA`
- `DBT_ATHENA_S3_STAGING_DIR`
- `DBT_ATHENA_WORKGROUP`

`DBT_ATHENA_S3_STAGING_DIR` should point to a writable staging prefix, for example:

```text
s3://my-bucket/dbt-athena-staging/
```

## Setup

Start the stack:

```bash
bash setup.sh
```

Useful commands:

```bash
make ps
make logs
make logs-webserver
make logs-scheduler
make dbt-debug
make dbt-ls
```

## Airflow DAGs

### Ingestion

- `titled_players_daily`
- `player_archives_incremental_daily`
- `player_games_current_month_daily`
- `player_games_backfill`
- `bootstrap_legacy_raw_state`

### Bronze Parquet

- `bronze_roster_daily`
- `bronze_games_current_month_daily`
- `bronze_games_month_backfill`

### dbt Silver

- `dbt_silver_current_daily`
- `dbt_silver_backfill`

### dbt Gold

- `dbt_gold_current_daily`
- `dbt_gold_backfill`

## First Deployment Order

1. Bootstrap the state layer from the existing raw bucket:

```bash
make trigger-bootstrap
```

2. Run the ingestion flow:

```bash
make trigger-core
```

3. If historical raw game months still need to be drained from the queue:

```bash
make trigger-backfill
```

4. Build the bronze parquet layer:

```bash
make trigger-bronze
```

5. Build silver in dbt:

```bash
make trigger-dbt-silver
```

6. Build gold in dbt:

```bash
make trigger-dbt-gold
```

## Manual Backfills

### Bronze Month Backfill

Single month:

```bash
make trigger-bronze-backfill MONTH_KEY=2026-02
```

Year:

```bash
make trigger-bronze-backfill BRONZE_CONF='{"year":"2026"}'
```

Range:

```bash
make trigger-bronze-backfill BRONZE_CONF='{"start_month":"2025-01","end_month":"2025-12"}'
```

### Silver dbt Backfill

Single month:

```bash
make trigger-dbt-silver-backfill MONTH_KEY=2026-02
```

Year:

```bash
make trigger-dbt-silver-backfill DBT_SILVER_CONF='{"year":"2026"}'
```

Range:

```bash
make trigger-dbt-silver-backfill DBT_SILVER_CONF='{"start_month":"2025-01","end_month":"2025-12"}'
```

### Gold dbt Backfill

Single month:

```bash
make trigger-dbt-gold-backfill MONTH_KEY=2026-02
```

Year:

```bash
make trigger-dbt-gold-backfill DBT_GOLD_CONF='{"year":"2026"}'
```

Range:

```bash
make trigger-dbt-gold-backfill DBT_GOLD_CONF='{"start_month":"2025-01","end_month":"2025-12"}'
```

For large historical rebuilds, prefer year-sized or smaller backfill windows. Athena Hive incremental models are partition-based, so smaller runs are safer and easier to retry.

## Airflow UI Parameters

The manual backfill DAGs accept parameters directly in the Airflow UI when using `Trigger DAG`.

Supported fields:

- `month_key`
- `month_keys`
- `year`
- `years`
- `start_month`
- `end_month`

Examples:

- `month_key = 2026-02`
- `month_keys = ["2026-01", "2026-02", "2026-03"]`
- `year = 2026`
- `years = ["2025", "2026"]`
- `start_month = 2025-01` and `end_month = 2025-12`

## Current Analytical Outputs

The modeled stack now supports these analyses cleanly:

- activity trends by title and month
- rating volatility by title and month
- white vs black performance by title and month

The next layer can extend from the same silver facts into:

- title-vs-title matchups
- opening performance by title
- roster churn by title
- head-to-head marts
- PGN-derived features after a separate parsing step

## Notes

- The project reuses the legacy raw bucket layout instead of re-downloading historical Chess.com data.
- Bronze backfills and dbt backfills are month-scoped so the stack can be controlled safely on a modest VPS.
- The silver layer no longer contains pre-aggregated monthly marts.
- Gold no longer depends on a `player_month` shortcut table; it is built from `silver_player_game_facts`.
