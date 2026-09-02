[English](README.md) | [Русский](README.ru.md)

# Data Engineering Tech Radar

A pet project for tracking the activity of open-source data engineering technologies.

The project will ingest public GitHub events from GH Archive, store historical data in a lakehouse, transform it into analytical data marts, and visualize technology trends in Tableau.

## Goal

Identify which data engineering technologies are gaining or losing momentum based on observable GitHub activity.

## Current functionality

The first pipeline stage downloads one hourly [GH Archive](https://www.gharchive.org/) file into the local raw data zone.

```text
GH Archive → HTTP streaming → temporary .part file → atomic rename → partitioned raw file
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

`--hour` must specify an exact timezone-aware hour. `Z` means UTC. Existing raw files are not downloaded again. The local `data/` directory is excluded from Git.