# Chess Incremental Data Platform

An Airflow + S3 (MinIO) + dbt + DuckDB data engineering project for Chess.com titled-player ingestion.

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
              +-----------------------------------------------+
              |              chess_daily DAG                   |
              |-----------------------------------------------|
              | snapshot_rosters >> refresh_archives           |
              |   >> sync_current_month_games                 |
              +----------+------------------------------------+
                         |  triggers
                         v
              +-----------------------------------------------+
              | chess_bronze >> chess_silver >> chess_gold      |
              +----------+------------------------------------+
                         |
                         v
              +-----------------------------------------------+
              |                S3 Bucket                       |
              |-----------------------------------------------|
              | raw/          raw JSON from API               |
              | state/        incremental control manifests   |
              | warehouse/    bronze / silver / gold parquet  |
              +----------+------------------------------------+
                         |
                         v
              +-----------------------------------------------+
              |          Streamlit Dashboard                   |
              |-----------------------------------------------|
              | DuckDB queries over gold parquet in S3        |
              +-----------------------------------------------+
```

## S3 Layout

### Raw

```text
raw/
├── titled_players/{TITLE}/{YYYY-MM-DD}.json
├── player_archives/{username}/{YYYY-MM-DD}.json
└── player_games/year={YYYY}/month={MM}/username={username}.json
```

### State

```text
state/
├── title_state/title={TITLE}/current.json
└── player_index/username_prefix={xx}/username={username}/index.json
```

### Warehouse

```text
warehouse/
├── bronze/
│   ├── roster_daily/snapshot_date=YYYY-MM-DD/title=GM/part-000.parquet
│   ├── games_core/year=YYYY/month=MM/bucket=00/part-000.parquet
│   └── player_game_facts/year=YYYY/month=MM/bucket=00/part-000.parquet
├── silver/
│   ├── roster_daily/snapshot_date=YYYY-MM-DD/part.parquet
│   ├── games_core/year=YYYY/month=MM/part.parquet
│   └── player_game_facts/year=YYYY/month=MM/part.parquet
└── gold/
    ├── title_month_activity/year=YYYY/month=MM/part.parquet
    ├── title_month_rating_volatility/year=YYYY/month=MM/part.parquet
    ├── title_month_color_performance/year=YYYY/month=MM/part.parquet
    ├── head_to_head_games/year=YYYY/month=MM/part.parquet
    ├── head_to_head_monthly/year=YYYY/month=MM/part.parquet
    └── head_to_head_summary/part.parquet
```

## Airflow DAGs

One DAG per function, chained via cross-DAG triggers.

### `chess_daily` (scheduled: `0 0 * * *`)

Daily ingestion from Chess.com API. Each task iterates all titles internally.
Triggers `chess_bronze` on completion.

```text
snapshot_rosters >> refresh_archives >> sync_current_month_games >> trigger_chess_bronze
```

### `chess_backfill` (manual trigger)

Drains each title's queued historical-month backfill from the Chess.com API
into raw. One task, all titles.

```text
backfill_games
```

### `chess_bronze` (triggered by `chess_daily`; also manual)

Bronze parquet materialization. With no conf, runs against the current month.
Accepts `month_key` / `month_keys` / `year` / `years` / `start_month` /
`end_month` to re-materialize historical months. Triggers `chess_silver`.
Raw files are kept in place (no archival).

```text
materialize_rosters >> materialize_games >> trigger_chess_silver
```

### `chess_silver` (triggered by `chess_bronze`; also manual)

dbt silver transformations. Accepts the same month conf as `chess_bronze`.
Triggers `chess_gold`.

```text
build_silver >> trigger_chess_gold
```

### `chess_gold` (triggered by `chess_silver`; also manual)

dbt gold transformations. Accepts the same month conf as `chess_bronze`.

```text
build_gold
```

### `chess_bootstrap` (manual, one-time)

Initialize state files from existing raw S3 layout.

## dbt Project

```text
dbt/chess_medallion/
├── models/
│   ├── staging/     ephemeral views over bronze parquet
│   ├── silver/      cleaned fact tables (external parquet)
│   └── gold/        analytical marts (external parquet)
└── macros/
    ├── filters.sql
    └── locations.sql
```

## Project Structure

```text
chess_incremental_pipeline/
├── airflow/dags/          6 DAG files
├── dbt/                   dbt project + profiles
├── scripts/               Python services
├── ui/                    Streamlit dashboard
├── docker-compose.yml
├── Dockerfile
├── Makefile
├── requirements.txt
├── setup.sh
└── .env.example
```

## Prerequisites

- Docker and Docker Compose on the VPS
- S3-compatible storage (AWS S3 or MinIO)
- An existing S3 bucket

## Environment

```bash
cp .env.example .env
```

Key variables:

- `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`
- `S3_BUCKET`, `S3_ENDPOINT_URL`
- `RAW_PREFIX` (default: `raw`)
- `STATE_PREFIX` (default: `state`)
- `WAREHOUSE_PREFIX` (default: `warehouse`)
- `ARCHIVE_PREFIX` (default: `archive`)
- `PARQUET_BUCKET_COUNT` (default: `16`)

## Setup

```bash
bash setup.sh
make build
make up
```

Useful commands:

```bash
make ps
make logs
make logs-scheduler
make trigger-daily
make trigger-bronze MONTH_KEY=2026-02
make trigger-silver MONTH_KEY=2026-02
make trigger-gold MONTH_KEY=2026-02
make trigger-backfill
make trigger-bootstrap
make streamlit-up
make dbt-debug
make dbt-ls
```

## First Deployment

1. Bootstrap state from existing raw data:

```bash
make trigger-bootstrap
```

2. Run the daily pipeline:

```bash
make trigger-ingest
```

3. Backfill historical months (end-to-end bronze >> silver >> gold):

```bash
make trigger-backfill MONTH_KEY=2026-02
make trigger-backfill CONF='{"year":"2025"}'
make trigger-backfill CONF='{"start_month":"2025-01","end_month":"2025-12"}'
```

## Backfill Parameters

The `chess_backfill` DAG accepts these parameters via Airflow UI or `--conf`:

- `month_key`: single month (e.g. `2026-02`)
- `month_keys`: array (e.g. `["2026-01", "2026-02"]`)
- `year`: single year (e.g. `2026`)
- `years`: array (e.g. `["2025", "2026"]`)
- `start_month` + `end_month`: range

## Streamlit Dashboard

```bash
make streamlit-up
```

Open `http://<your-vps-ip>:8501`. Tabs: Activity Trends, Rating Volatility, White vs Black, Head to Head, Player Explorer.

Queries gold/silver parquet via DuckDB -- no Chess.com API traffic.

## Analytical Outputs

- Activity trends by title and month
- Rating volatility by title and month
- White vs black performance by title and month
- Player-vs-player head-to-head across full historical dataset
- Per-player game explorer with monthly/daily drill-down
