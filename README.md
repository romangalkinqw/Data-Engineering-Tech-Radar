[English](README.md) | [Русский](README.ru.md)

# Data Engineering Tech Radar

A pet project for tracking the activity of open-source data engineering technologies.

The project ingests public GitHub events from GH Archive, stores historical data in a lakehouse, transforms raw events into typed analytical records, and will use data marts and Tableau dashboards to visualize technology trends.

## Goal

Identify which data engineering technologies are gaining or losing momentum based on observable GitHub activity.

## Current functionality

The daily pipeline is orchestrated by Dagster and processes 24 hourly GH Archive deliveries through Raw, Bronze, Silver, and Gold layers:

```text
Dagster daily partition
  → GH Archive
  → HTTP streaming
  → partitioned Raw .json.gz
  → streaming JSON parser
  → bounded PyArrow batches
  → Apache Iceberg Bronze table
  → typed event transformation
  → date-partitioned Apache Iceberg Silver table
  → technology catalog classification
  → date-partitioned Apache Iceberg Gold activity table
```

The Bronze layer preserves source payloads and ingestion lineage. The Silver layer provides typed, analysis-ready columns and extracts event-specific values such as:

- `action` for events that expose an action;
- `commit_count` for `PushEvent`;
- `is_merged` for `PullRequestEvent`;
- `event_date` for Iceberg partitioning.

Source coordinates remain available through `source_file`, `source_line_number`, and `archive_hour`.

The Gold layer aggregates Silver events into one daily row per tracked technology. It currently exposes total events, unique actors, pushes, commits, pull request activity, merged pull requests, issues, stars, forks, and releases.

## Quick start

Requirements:

- Python 3.14
- uv

Install the project and its locked dependencies:

```bash
uv sync --locked
```

Run the complete Raw-to-Gold pipeline for one UTC day:

```bash
uv run de-tech-radar run-gharchive-day \
  --date 2015-01-01
```

The command expands the date into 24 hourly GH Archive deliveries, processes every hour through Raw, Bronze, and Silver, and then atomically rebuilds the Gold date partition. It prints an operational summary:

```text
DailyPipelineResult(archive_count=24, bronze_rows=218939, silver_rows=218939, gold_rows=2)
```

The pipeline is restart-safe. Existing Raw files are reused, committed Bronze and Silver sources are skipped, and Gold is recomputed without duplicate rows. The individual stage commands below remain available for debugging and targeted backfills.

Download one hourly archive:

```bash
uv run de-tech-radar ingest-gharchive \
  --hour 2015-01-01T15:00:00Z \
  --raw-root data/raw
```

Result:

```text
data/raw/gharchive/archive_date=2015-01-01/archive_hour=15/2015-01-01-15.json.gz
```

`--hour` must specify an exact timezone-aware hour. `Z` means UTC. Existing Raw files are not downloaded again. The local `data/` directory is excluded from Git.

Load the downloaded archive into the local Iceberg Bronze table:

```bash
uv run de-tech-radar load-gharchive-bronze \
  --archive-path data/raw/gharchive/archive_date=2015-01-01/archive_hour=15/2015-01-01-15.json.gz \
  --hour 2015-01-01T15:00:00Z
```

Transform the Bronze source into the Iceberg Silver table:

```bash
uv run de-tech-radar load-gharchive-silver \
  --source-file 2015-01-01-15.json.gz
```

Build daily Gold activity metrics from Silver:

```bash
uv run de-tech-radar build-gold-daily-activity \
  --date 2015-01-01
```

The local development lakehouse uses a SQLite catalog at `data/lakehouse/catalog.db` and stores Iceberg metadata and Parquet files under `data/lakehouse/warehouse`.

The Bronze and Silver loading commands print the number of written rows. Successfully committed source files are recorded in Iceberg snapshot metadata, so sequential retries return `0` without creating duplicates. All rows belonging to one source file are published atomically. An unknown Bronze `source_file` is rejected instead of being treated as a successful empty load.

The Gold command recomputes the complete selected date and atomically replaces its Iceberg partition. Repeating the command produces the same final rows without duplicates, while late-arriving Silver events are included on the next run. Previous table states remain available through Iceberg snapshot history.

## Orchestration

Dagster exposes the complete daily pipeline as the partitioned `daily_technology_activity` asset. Each partition represents one UTC date. The asset is executed by `daily_technology_activity_job`, and `daily_technology_activity_schedule` targets the latest completed partition every day at 02:00 UTC.

Filesystem locations are supplied through the typed `RadarPaths` configurable resource and are resolved from the project root, so runs are independent of the worker process current directory. Materializations publish `archive_count`, `bronze_rows`, `silver_rows`, and `gold_rows` metadata in the Dagster UI.

Validate the Dagster code location:

```bash
uv run dg check defs
```

Start the local Dagster UI:

```bash
uv run dg dev
```

Open `http://127.0.0.1:3000` and materialize a date partition. Repeating the `2015-01-01` partition writes `0` new Bronze and Silver rows while safely rebuilding the same two Gold rows.

## Data quality

Blocking Dagster Asset Checks validate every materialized Gold result:

- `unique_gold_keys` requires at most one row per `(technology_id, activity_date)` business key;
- `valid_gold_metrics` rejects negative counters and inconsistent relationships, including unique actors exceeding total events, merged pull requests exceeding pull requests, or event subtype counts exceeding total events.

Each check reads only the selected Gold date and publishes row and violation counts as Dagster metadata. For the `2015-01-01` materialization, both checks pass with two Gold rows, zero duplicate keys, and zero invalid metric rows. A failed blocking check marks the run unsuccessful instead of silently exposing invalid data to downstream BI consumers.

## Technology catalog

`config/technologies.toml` is the version-controlled source of truth that maps stable technology IDs, display names, and categories to GitHub repositories. The initial catalog covers orchestration, transformation, processing, streaming, table formats, query engines, ingestion, and data quality tools.

The typed catalog loader normalizes repository names, validates the `owner/name` format, and rejects duplicate technology IDs or repository assignments. It also builds a repository index for constant-time classification of Silver events. This mapping drives the Gold activity metrics.

## Quality checks

```bash
uv run ruff format --check .
uv run ruff check .
uv run mypy
uv run pytest
uv run dg check defs
```

The same checks, including Dagster definition validation, run in GitHub Actions for pull requests and protect the `main` branch.
