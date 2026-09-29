[English](README.md) | [Русский](README.ru.md)

# Data Engineering Tech Radar

A pet project for tracking the activity of open-source data engineering technologies.

The project ingests public GitHub events from GH Archive, stores historical data in a lakehouse, transforms it into analytical data marts, and visualizes technology trends in Tableau.

## Goal

Identify which data engineering technologies are gaining or losing momentum based on observable GitHub activity.

## Current functionality

The pipeline downloads one hourly GH Archive file into the local Raw zone and loads its events into an Apache Iceberg Bronze table.

```text
GH Archive
  → HTTP streaming
  → partitioned Raw .json.gz
  → streaming parser
  → bounded PyArrow batches
  → Apache Iceberg Bronze table
```

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

The local development lakehouse uses a SQLite catalog at `data/lakehouse/catalog.db` and stores Iceberg metadata and Parquet files under `data/lakehouse/warehouse`.

The command prints the number of written events. Successfully committed source files are recorded in Iceberg snapshot metadata, so retrying the same archive returns `0`. All batches belonging to one archive are published atomically.
