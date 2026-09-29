[English](README.md) | [Русский](README.ru.md)

# Data Engineering Tech Radar

A pet project for tracking the activity of open-source data engineering technologies.

The project ingests public GitHub events from GH Archive, stores historical data in a lakehouse, transforms raw events into typed analytical records, and will use data marts and Tableau dashboards to visualize technology trends.

## Goal

Identify which data engineering technologies are gaining or losing momentum based on observable GitHub activity.

## Current functionality

The pipeline processes one hourly GH Archive delivery through Raw, Bronze, and Silver layers:

```text
GH Archive
  → HTTP streaming
  → partitioned Raw .json.gz
  → streaming JSON parser
  → bounded PyArrow batches
  → Apache Iceberg Bronze table
  → typed event transformation
  → date-partitioned Apache Iceberg Silver table
```

The Bronze layer preserves source payloads and ingestion lineage. The Silver layer provides typed, analysis-ready columns and extracts event-specific values such as:

- `action` for events that expose an action;
- `commit_count` for `PushEvent`;
- `is_merged` for `PullRequestEvent`;
- `event_date` for Iceberg partitioning.

Source coordinates remain available through `source_file`, `source_line_number`, and `archive_hour`.

## Quick start

Requirements:

- Python 3.14
- uv

Install the project and its locked dependencies:

```bash
uv sync --locked
```

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
  --source-file data/raw/gharchive/archive_date=2015-01-01/archive_hour=15/2015-01-01-15.json.gz
```

The local development lakehouse uses a SQLite catalog at `data/lakehouse/catalog.db` and stores Iceberg metadata and Parquet files under `data/lakehouse/warehouse`.

Both table-loading commands print the number of written rows. Successfully committed source files are recorded in Iceberg snapshot metadata, so sequential retries return `0` without creating duplicates. All rows belonging to one source file are published atomically. An unknown Bronze `source_file` is rejected instead of being treated as a successful empty load.

## Quality checks

```bash
uv run ruff format --check .
uv run ruff check .
uv run mypy
uv run pytest
```

The same checks run in GitHub Actions for pull requests and protect the `main` branch.
