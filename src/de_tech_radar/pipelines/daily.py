import asyncio
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path

import httpx
from pyiceberg.table import Table

from de_tech_radar.ingestion.gharchive import download_archive
from de_tech_radar.pipelines.bronze_silver import (
    load_bronze_source_to_silver,
)
from de_tech_radar.pipelines.gharchive_bronze import (
    load_archive_to_bronze,
)
from de_tech_radar.pipelines.silver_gold import (
    load_silver_date_to_gold,
)
from de_tech_radar.technologies.catalog import Technology


@dataclass(frozen=True, slots=True)
class DailyPipelineResult:
    archive_count: int
    bronze_rows: int
    silver_rows: int
    gold_rows: int


def build_archive_hours(
    activity_date: date,
) -> tuple[datetime, ...]:
    """Build all UTC archive hours for one date."""
    return tuple(
        datetime(
            activity_date.year,
            activity_date.month,
            activity_date.day,
            hour,
            tzinfo=UTC,
        )
        for hour in range(24)
    )


async def download_archive_hours(
    *,
    archive_hours: Iterable[datetime],
    raw_root: Path,
    client: httpx.Client,
    max_concurrency: int = 4,
) -> tuple[Path, ...]:
    """Download archive hours with bounded concurrency."""
    if max_concurrency < 1:
        raise ValueError("max_concurrency must be at least 1")

    hours = tuple(archive_hours)
    semaphore = asyncio.Semaphore(max_concurrency)

    async def download_one(
        archive_hour: datetime,
    ) -> Path:
        async with semaphore:
            return await asyncio.to_thread(
                download_archive,
                archive_hour=archive_hour,
                raw_root=raw_root,
                client=client,
            )

    return tuple(await asyncio.gather(*(download_one(archive_hour) for archive_hour in hours)))


def run_archive_hours(
    *,
    max_download_concurrency: int = 4,
    archive_hours: Iterable[datetime],
    raw_root: Path,
    bronze_table: Table,
    silver_table: Table,
    gold_table: Table,
    technologies: Iterable[Technology],
    client: httpx.Client,
) -> DailyPipelineResult:
    """Run Raw-to-Gold processing for archive hours."""
    hours = tuple(archive_hours)

    if not hours:
        raise ValueError("at least one archive hour is required")

    if any(hour.tzinfo is None or hour.utcoffset() is None for hour in hours):
        raise ValueError("archive hours must be timezone-aware")

    activity_dates = {hour.astimezone(UTC).date() for hour in hours}

    if len(activity_dates) != 1:
        raise ValueError("all archive hours must belong to the same UTC date")

    activity_date = activity_dates.pop()
    bronze_rows = 0
    silver_rows = 0

    archive_paths = asyncio.run(
        download_archive_hours(
            archive_hours=hours,
            raw_root=raw_root,
            client=client,
            max_concurrency=max_download_concurrency,
        )
    )

    for archive_hour, archive_path in zip(
        hours,
        archive_paths,
        strict=True,
    ):
        bronze_rows += load_archive_to_bronze(
            archive_path=archive_path,
            archive_hour=archive_hour,
            table=bronze_table,
        )
        silver_rows += load_bronze_source_to_silver(
            bronze_table=bronze_table,
            silver_table=silver_table,
            source_file=archive_path.name,
        )

    gold_rows = load_silver_date_to_gold(
        silver_table=silver_table,
        gold_table=gold_table,
        activity_date=activity_date,
        technologies=technologies,
    )

    return DailyPipelineResult(
        archive_count=len(hours),
        bronze_rows=bronze_rows,
        silver_rows=silver_rows,
        gold_rows=gold_rows,
    )


def run_daily_pipeline(
    *,
    max_download_concurrency: int = 4,
    activity_date: date,
    raw_root: Path,
    bronze_table: Table,
    silver_table: Table,
    gold_table: Table,
    technologies: Iterable[Technology],
    client: httpx.Client,
) -> DailyPipelineResult:
    """Run the complete Raw-to-Gold pipeline for one UTC day."""
    return run_archive_hours(
        max_download_concurrency=max_download_concurrency,
        archive_hours=build_archive_hours(activity_date),
        raw_root=raw_root,
        bronze_table=bronze_table,
        silver_table=silver_table,
        gold_table=gold_table,
        technologies=technologies,
        client=client,
    )
