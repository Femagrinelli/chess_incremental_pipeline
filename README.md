# Chess Incremental Data Platform

An Airflow + AWS S3 data engineering project for Chess.com titled-player ingestion.

This version is designed around two goals: keep the raw layer complete while avoiding wasteful daily API calls, and materialize a reusable silver layer in parquet for downstream analytics.

## What This Version Optimizes

- `titled_players` is stored as a true daily CDC snapshot.
- `player_archives` is refreshed only for new, active, or stale players.
- `player_games` is fetched daily only for players who are active in the current month.
- Historical months are drained through a controlled backfill queue instead of re-scanning everyone every day.
- Daily DAGs use deterministic keys plus manifest files, so they do not need large `LIST` operations in S3.
- The existing legacy `raw/` bucket layout is reused instead of re-downloading history.

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
                    |            Airflow DAGs               |
                    |---------------------------------------|
                    | Bronze: titled / archives / games     |
                    | Silver: roster / player_games / month  |
                    +----------+----------------------------+
                               |
                +--------------+------------------+----------------------+
                |                                 |                      |
                v                                 v                      v
   +-------------------------------+   +-------------------------------+   +-------------------------------+
   | S3 Raw / Bronze               |   | S3 State / Control            |   | S3 Silver / Parquet          |
   |-------------------------------|   |-------------------------------|   |-------------------------------|
   | titled_players CDC snapshots  |   | title_state/title=GM          |   | title_roster_daily           |
   | player_archives change-only   |   | player_index/username=...     |   | player_games                 |
   | player_games by year/month    |   | backfill counters + ETags     |   | player_month                 |
   +-------------------------------+   +-------------------------------+   +-------------------------------+
                                                                                         |
                                                                                         v
                                                                            +-------------------------------+
                                                                            | Future Gold / Analytics       |
                                                                            |-------------------------------|
                                                                            | title trends                  |
                                                                            | white vs black performance    |
                                                                            | rating volatility             |
                                                                            +-------------------------------+
```

## Why This Structure Is Better

### 1. Daily roster is still CDC and stays compatible with the existing bucket layout

Every title endpoint is fetched every day and written to:

```text
raw/titled_players/GM/2026-03-29.json
```

This gives you a full historical roster trail.

### 2. Archives are not fetched for every player every day

The hot manifest for each title is stored at:

```text
state/chess_com/title_state/title=GM/current.json
```

That manifest tracks:

- first time a player was seen
- last roster snapshot date
- last time archives were checked
- whether the player is active in the current month
- the latest ETag and Last-Modified for current-month games
- whether historical backfill is still pending

Because of that manifest, the archive DAG only refreshes:

- newly discovered players
- players who already have current-month activity
- players whose archive state is stale

### 3. Current-month games are synced only for active players

If a player does not have the current month in their archive list, the daily games DAG skips them.

If they do, the daily games DAG calls:

```text
/player/{username}/games/{YYYY}/{MM}
```

using conditional headers (`If-None-Match`, `If-Modified-Since`) when possible.

If Chess.com returns `304 Not Modified`, the pipeline updates only state, not the raw object.

### 4. Historical months are queue-based

Instead of re-reading old months over and over, archive changes create a small per-player queue in:

```text
state/chess_com/player_index/username_prefix=ve/username=veselintopalov359/index.json
```

The backfill DAG drains that queue gradually, with caps controlled by:

- `BACKFILL_PLAYERS_PER_RUN`
- `BACKFILL_MONTHS_PER_PLAYER`

That makes the first bootstrap safe on a small VPS and lets you speed up later if needed.

## Legacy Raw Layout Reused By This Project

```text
chess-lake/
├── raw/
│   ├── titled_players/
│   │   └── GM/
│   │       └── 2026-03-29.json
│   ├── player_archives/
│   │   └── veselintopalov359/
│   │       ├── 2026-03-29.json
│   │       └── 2026/
│   │           └── 02.json
│   └── player_games/
│       └── veselintopalov359/
│           └── 2022/
│               └── 09.json
└── state/
    └── chess_com/
        ├── title_state/
        │   └── title=GM/current.json
        └── player_index/
            └── username_prefix=ve/username=veselintopalov359/index.json
```

Notes:

- `titled_players` stays in the exact legacy layout.
- `player_games` stays in the exact legacy layout.
- `player_archives` is read from both legacy variants during bootstrap:
  - `raw/player_archives/{username}/{YYYY-MM-DD}.json`
  - `raw/player_archives/{username}/{YYYY}/{MM}.json`
- New incremental refreshes write the daily archive snapshot style only when the archive list changes.

## Silver Layout Produced By This Project

```text
chess-lake/
└── silver/
    └── chess_com/
        ├── title_roster_daily/
        │   └── snapshot_date=2026-03-29/
        │       └── title=GM/part-000.parquet
        ├── player_games/
        │   └── year=2026/
        │       └── month=03/
        │           └── title=GM/
        │               └── bucket=00/part-000.parquet
        └── player_month/
            └── year=2026/
                └── month=03/
                    └── title=GM/
                        └── bucket=00/part-000.parquet
```

The silver datasets are organized as follows:

- `title_roster_daily`: one row per player per title snapshot day.
- `player_games`: one row per titled player per game, sourced from that player's raw monthly file.
- `player_month`: one row per titled player per month, pre-aggregated for trend analysis.

The `player_games` and `player_month` datasets are partitioned by month and title so the daily materialization can run in smaller chunks on a modest VPS.

## Project Structure

```text
chess_incremental_pipeline/
├── Dockerfile
├── docker-compose.yml
├── .env.example
├── Makefile
├── setup.sh
├── requirements.txt
├── README.md
├── airflow/
│   └── dags/
│       ├── bootstrap_legacy_raw_state.py
│       ├── titled_players_daily.py
│       ├── player_archives_incremental_daily.py
│       ├── player_games_current_month_daily.py
│       ├── player_games_backfill.py
│       ├── silver_title_roster_daily.py
│       ├── silver_games_current_month_daily.py
│       └── silver_games_month_backfill.py
└── scripts/
    ├── config.py
    ├── chess_client.py
    ├── storage_client.py
    ├── state_store.py
    ├── ingestion_service.py
    └── silver_service.py
```

## DAG Design

### `bootstrap_legacy_raw_state`

- Schedule: manual only
- One task per title.
- Reads the latest legacy `raw/titled_players/{TITLE}/{YYYY-MM-DD}.json`.
- Reads the latest legacy archive object and stored game months for each current player.
- Creates the new `state/chess_com/...` files without re-downloading Chess.com history.

### `titled_players_daily`

- Schedule: `00:05 UTC`
- One task per title.
- Calls `/pub/titled/{title}`.
- Stores a full raw CDC snapshot in the existing legacy path.
- Updates the title manifest with new and removed players.

### `player_archives_incremental_daily`

- Schedule: `00:35 UTC`
- One task per title.
- Refreshes archives only for new, active, or stale players.
- Stores raw archive snapshots only when the archive list changes.
- Updates backfill queue counts.

### `player_games_current_month_daily`

- Schedule: `01:10 UTC`
- One task per title.
- Only processes players with current-month activity.
- Uses `ETag` / `Last-Modified` for lighter syncs.
- Overwrites the current month file only when the month payload changed.

### `player_games_backfill`

- Schedule: `02:30 UTC`
- One task per title.
- Processes only queued historical months.
- Safe to trigger manually if you want to accelerate the bootstrap.

### `silver_title_roster_daily`

- Schedule: `00:20 UTC`
- One task per title.
- Reads the same daily raw titled-player snapshot and writes parquet for the roster CDC layer.

### `silver_games_current_month_daily`

- Schedule: `04:15 UTC`
- One task per title, chained sequentially to keep VPS load predictable.
- Reads current-month raw game files for titled players in that title and writes:
  - `player_games`
  - `player_month`
- Uses a player-centric silver shape instead of a single global game dedup in the hot path.

### `silver_games_month_backfill`

- Schedule: manual only
- Single task.
- Rebuilds selected historical months from raw JSON already stored in S3.
- Processes month selections title by title to avoid one large all-player task.
- The Airflow UI trigger form exposes `month_key`, `month_keys`, `year`, `years`, `start_month`, and `end_month` as DAG params.
- Accepts any of the following DAG run config patterns:

```json
{"month_key": "2026-02"}
```

```json
{"month_keys": ["2026-01", "2026-02", "2026-03"]}
```

```json
{"year": "2026"}
```

```json
{"years": ["2025", "2026"]}
```

```json
{"start_month": "2025-01", "end_month": "2025-12"}
```

## Step-by-Step VPS Setup

### 1. Enter the project folder

```bash
cd /root/chess_incremental_pipeline
```

### 2. Create a local environment file

```bash
cp .env.example .env
```

Update at least these values:

- `POSTGRES_PASSWORD`
- `AIRFLOW_ADMIN_PASSWORD`
- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `S3_BUCKET`
- `CHESS_API_USER_AGENT`
- `FERNET_KEY`
- `SECRET_KEY`
- `SILVER_PREFIX`
- `SILVER_BUCKET_COUNT`
- `AIRFLOW__CORE__DAG_FILE_PROCESSOR_TIMEOUT`
- `AIRFLOW__SCHEDULER__STALE_DAG_THRESHOLD`

Important:

- `S3_BUCKET` should already exist in AWS.
- Leave `S3_ENDPOINT_URL` blank for AWS S3. Only set this value if you later point the project to another S3-compatible object store like MinIO.
- On small VPS instances, keep `AIRFLOW__CORE__DAG_FILE_PROCESSOR_TIMEOUT` higher than `AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT`. The defaults in this repo already do that.

### 3. Start the stack

```bash
bash setup.sh
```

This script will:

- generate Airflow keys if needed
- build the Airflow image
- start PostgreSQL, Airflow webserver and scheduler

### 4. Run the one-time bootstrap first

- Build state from an existing raw bucket:

```bash
make trigger-bootstrap
```

That bootstrap reads the legacy raw files already in S3 and creates the `state/` layer used by the new incremental DAGs.

### 5. Open the services

- Airflow UI: `http://<your-vps-ip>:8080`
- If running on your machine: 'localhost:8080'

### 6. Run the new daily flow

For the core daily flow:

```bash
make trigger-core
```

For historical queue draining:

```bash
make trigger-backfill
```

For the silver daily flow:

```bash
make trigger-silver
```

For a manual silver month rebuild:

```bash
make trigger-silver-backfill
```

For a specific month:

```bash
make trigger-silver-backfill MONTH_KEY=2026-02
```

For a year:

```bash
make trigger-silver-backfill SILVER_CONF='{"year":"2026"}'
```

For a range:

```bash
make trigger-silver-backfill SILVER_CONF='{"start_month":"2025-01","end_month":"2025-12"}'
```

When triggering from the Airflow UI, these same fields can be filled directly in the DAG params form, or pasted as JSON in the trigger config dialog.

## First Bootstrap Strategy

The best first-load strategy is:

1. Run `bootstrap_legacy_raw_state` once
2. Run `titled_players_daily`
3. Run `player_archives_incremental_daily`
4. Let `player_games_backfill` drain any missing history gradually
5. Run `player_games_current_month_daily`
6. Run `silver_title_roster_daily`
7. Run `silver_games_current_month_daily`

## Query Layer Options

The silver layer is stored as parquet so it can be queried by several tools later:

- DuckDB for local SQL on parquet, especially for notebooks and ad hoc analysis.
- Athena for serverless SQL directly on S3.
- Polars or Pandas for Python-based exploration.
- dbt-duckdb or dbt-athena if the project later moves toward a model-driven analytics layer.

## DuckDB Queries

DuckDB is wired into the Airflow image so the VPS can query silver parquet directly in S3.

After pulling changes, rebuild the image:

```bash
docker compose build
docker compose up -d
```

List the bundled sample SQL files:

```bash
make duckdb-list
```

Run a sample query file:

```bash
make duckdb-file SQL_FILE=player_month_sample.sql
```

Run an inline query:

```bash
make duckdb-query SQL="SELECT COUNT(*) FROM read_parquet('s3://your-bucket/silver/chess_com/player_month/year=2026/month=03/title=GM/bucket=*/part-000.parquet')"
```

The helper script automatically reads `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`, `S3_BUCKET`, and `SILVER_PREFIX` from the container environment.

## Notes

This design is aligned with Chess.com PubAPI behavior: the official docs note that endpoints include `ETag` and `Last-Modified`, can return `304 Not Modified`, refresh at most every 12 hours, and may rate-limit parallel calls.
