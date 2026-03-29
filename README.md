# Chess Incremental Data Platform

An Airflow + AWS S3 data engineering project for Chess.com titled-player ingestion.

This version is designed around one goal: keep the raw layer complete while avoiding wasteful daily API calls and unnecessary S3 operations.

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
                    | 1. titled_players_daily               |
                    | 2. player_archives_incremental_daily  |
                    | 3. player_games_current_month_daily   |
                    | 4. player_games_backfill              |
                    +----------+----------------------------+
                               |
                +--------------+------------------+
                |                                 |
                v                                 v
   +-------------------------------+   +-------------------------------+
   | S3 Raw / Bronze               |   | S3 State / Control            |
   |-------------------------------|   |-------------------------------|
   | titled_players CDC snapshots  |   | title_state/title=GM          |
   | player_archives change-only   |   | player_index/username=...     |
   | player_games by year/month    |   | backfill counters + ETags     |
   +-------------------------------+   +-------------------------------+
                               |
                               v
                    +----------------------------+
                    | Future Silver Layer        |
                    |----------------------------|
                    | aggregates                 |
                    | PGN extraction             |
                    | curated serving tables     |
                    +----------------------------+
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
│       └── player_games_backfill.py
└── scripts/
    ├── config.py
    ├── chess_client.py
    ├── storage_client.py
    ├── state_store.py
    └── ingestion_service.py
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

Important:

- `S3_BUCKET` should already exist in AWS.
- Leave `S3_ENDPOINT_URL` blank for AWS S3. Only set this value if you later point the project to another S3-compatible object store like MinIO.

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

## First Bootstrap Strategy

The best first-load strategy is:

1. Run `bootstrap_legacy_raw_state` once
2. Run `titled_players_daily`
3. Run `player_archives_incremental_daily`
4. Let `player_games_backfill` drain any missing history gradually
5. Run `player_games_current_month_daily` every day

## Notes

This design is aligned with Chess.com PubAPI behavior: the official docs note that endpoints include `ETag` and `Last-Modified`, can return `304 Not Modified`, refresh at most every 12 hours, and may rate-limit parallel calls.
