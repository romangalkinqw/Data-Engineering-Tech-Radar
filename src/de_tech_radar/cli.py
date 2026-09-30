import argparse
from collections.abc import Sequence
from datetime import date, datetime
from pathlib import Path
from typing import cast

import httpx

from de_tech_radar.ingestion.gharchive import download_archive
from de_tech_radar.lakehouse.catalog import open_local_catalog
from de_tech_radar.lakehouse.tables import (
    ensure_bronze_events_table,
    ensure_gold_daily_activity_table,
    ensure_silver_events_table,
)
from de_tech_radar.pipelines.bronze_silver import (
    load_bronze_source_to_silver,
)
from de_tech_radar.pipelines.daily import (
    DailyPipelineResult,
    run_daily_pipeline,
)
from de_tech_radar.pipelines.gharchive_bronze import (
    DEFAULT_BATCH_SIZE,
    load_archive_to_bronze,
)
from de_tech_radar.pipelines.silver_gold import (
    load_silver_date_to_gold,
)
from de_tech_radar.technologies.catalog import (
    load_technology_catalog,
)


def parse_args(
    argv: Sequence[str] | None = None,
) -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(prog="de-tech-radar")
    subparsers = parser.add_subparsers(dest="command", required=True)

    ingest_parser = subparsers.add_parser(
        "ingest-gharchive",
        help="Download one hourly GH Archive file",
    )
    ingest_parser.add_argument(
        "--hour",
        dest="archive_hour",
        type=_parse_archive_hour,
        required=True,
        help="UTC hour in ISO 8601 format",
    )
    ingest_parser.add_argument(
        "--raw-root",
        type=Path,
        default=Path("data/raw"),
        help="Root directory of the raw data zone",
    )

    load_bronze_parser = subparsers.add_parser(
        "load-gharchive-bronze",
        help="Load one raw GH Archive file into Iceberg Bronze",
    )
    load_bronze_parser.add_argument(
        "--archive-path",
        type=Path,
        required=True,
        help="Path to an hourly GH Archive .json.gz file",
    )
    load_bronze_parser.add_argument(
        "--hour",
        dest="archive_hour",
        type=_parse_archive_hour,
        required=True,
        help="UTC hour in ISO 8601 format",
    )
    load_bronze_parser.add_argument(
        "--catalog-path",
        type=Path,
        default=Path("data/lakehouse/catalog.db"),
        help="Path to the local SQLite Iceberg catalog",
    )
    load_bronze_parser.add_argument(
        "--warehouse-path",
        type=Path,
        default=Path("data/lakehouse/warehouse"),
        help="Path to the local Iceberg warehouse",
    )
    load_bronze_parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help="Maximum number of events per write batch",
    )
    load_silver_parser = subparsers.add_parser(
        "load-gharchive-silver",
        help="Transform one Bronze source file into Iceberg Silver",
    )
    load_silver_parser.add_argument(
        "--source-file",
        required=True,
        help="Bronze source_file value to transform",
    )
    load_silver_parser.add_argument(
        "--catalog-path",
        type=Path,
        default=Path("data/lakehouse/catalog.db"),
        help="Path to the local SQLite Iceberg catalog",
    )
    load_silver_parser.add_argument(
        "--warehouse-path",
        type=Path,
        default=Path("data/lakehouse/warehouse"),
        help="Path to the local Iceberg warehouse",
    )
    build_gold_parser = subparsers.add_parser(
        "build-gold-daily-activity",
        help="Build Gold technology activity for one date",
    )
    build_gold_parser.add_argument(
        "--date",
        dest="activity_date",
        type=_parse_date,
        required=True,
        help="Activity date in YYYY-MM-DD format",
    )
    build_gold_parser.add_argument(
        "--technology-catalog",
        dest="technology_catalog_path",
        type=Path,
        default=Path("config/technologies.toml"),
        help="Path to the technology catalog TOML file",
    )
    build_gold_parser.add_argument(
        "--catalog-path",
        type=Path,
        default=Path("data/lakehouse/catalog.db"),
        help="Path to the local SQLite Iceberg catalog",
    )
    build_gold_parser.add_argument(
        "--warehouse-path",
        type=Path,
        default=Path("data/lakehouse/warehouse"),
        help="Path to the local Iceberg warehouse",
    )
    run_day_parser = subparsers.add_parser(
        "run-gharchive-day",
        help="Run the complete Raw-to-Gold daily pipeline",
    )
    run_day_parser.add_argument(
        "--date",
        dest="activity_date",
        type=_parse_date,
        required=True,
        help="Activity date in YYYY-MM-DD format",
    )
    run_day_parser.add_argument(
        "--raw-root",
        type=Path,
        default=Path("data/raw"),
        help="Root directory of the raw data zone",
    )
    run_day_parser.add_argument(
        "--technology-catalog",
        dest="technology_catalog_path",
        type=Path,
        default=Path("config/technologies.toml"),
        help="Path to the technology catalog TOML file",
    )
    run_day_parser.add_argument(
        "--catalog-path",
        type=Path,
        default=Path("data/lakehouse/catalog.db"),
        help="Path to the local SQLite Iceberg catalog",
    )
    run_day_parser.add_argument(
        "--warehouse-path",
        type=Path,
        default=Path("data/lakehouse/warehouse"),
        help="Path to the local Iceberg warehouse",
    )
    return parser.parse_args(argv)


def execute_command(
    arguments: argparse.Namespace,
    client: httpx.Client,
) -> Path | int | DailyPipelineResult:
    """Execute a parsed CLI command."""
    if arguments.command == "ingest-gharchive":
        return download_archive(
            archive_hour=cast(datetime, arguments.archive_hour),
            raw_root=cast(Path, arguments.raw_root),
            client=client,
        )

    if arguments.command == "load-gharchive-bronze":
        with open_local_catalog(
            catalog_name="local",
            catalog_path=cast(Path, arguments.catalog_path),
            warehouse_path=cast(
                Path,
                arguments.warehouse_path,
            ),
        ) as catalog:
            table = ensure_bronze_events_table(catalog)
            return load_archive_to_bronze(
                archive_path=cast(
                    Path,
                    arguments.archive_path,
                ),
                archive_hour=cast(
                    datetime,
                    arguments.archive_hour,
                ),
                table=table,
                batch_size=cast(int, arguments.batch_size),
            )

    if arguments.command == "load-gharchive-silver":
        with open_local_catalog(
            catalog_name="local",
            catalog_path=cast(
                Path,
                arguments.catalog_path,
            ),
            warehouse_path=cast(
                Path,
                arguments.warehouse_path,
            ),
        ) as catalog:
            bronze_table = ensure_bronze_events_table(catalog)
            silver_table = ensure_silver_events_table(catalog)

            return load_bronze_source_to_silver(
                bronze_table=bronze_table,
                silver_table=silver_table,
                source_file=cast(
                    str,
                    arguments.source_file,
                ),
            )

    if arguments.command == "build-gold-daily-activity":
        technology_catalog_path = cast(
            Path,
            arguments.technology_catalog_path,
        )
        technologies = load_technology_catalog(technology_catalog_path)

        with open_local_catalog(
            catalog_name="local",
            catalog_path=cast(
                Path,
                arguments.catalog_path,
            ),
            warehouse_path=cast(
                Path,
                arguments.warehouse_path,
            ),
        ) as catalog:
            silver_table = ensure_silver_events_table(catalog)
            gold_table = ensure_gold_daily_activity_table(catalog)

            return load_silver_date_to_gold(
                silver_table=silver_table,
                gold_table=gold_table,
                activity_date=cast(
                    date,
                    arguments.activity_date,
                ),
                technologies=technologies,
            )
    if arguments.command == "run-gharchive-day":
        technologies = load_technology_catalog(
            cast(
                Path,
                arguments.technology_catalog_path,
            )
        )

        with open_local_catalog(
            catalog_name="local",
            catalog_path=cast(
                Path,
                arguments.catalog_path,
            ),
            warehouse_path=cast(
                Path,
                arguments.warehouse_path,
            ),
        ) as catalog:
            bronze_table = ensure_bronze_events_table(catalog)
            silver_table = ensure_silver_events_table(catalog)
            gold_table = ensure_gold_daily_activity_table(catalog)

            return run_daily_pipeline(
                activity_date=cast(
                    date,
                    arguments.activity_date,
                ),
                raw_root=cast(
                    Path,
                    arguments.raw_root,
                ),
                bronze_table=bronze_table,
                silver_table=silver_table,
                gold_table=gold_table,
                technologies=technologies,
                client=client,
            )
    raise ValueError(f"unsupported command: {arguments.command}")


def _parse_archive_hour(value: str) -> datetime:
    try:
        return datetime.fromisoformat(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError(f"invalid ISO 8601 datetime: {value}") from error


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError(f"invalid ISO 8601 date: {value}") from error


def main() -> None:
    """Run the command-line interface."""
    arguments = parse_args()

    with httpx.Client(
        follow_redirects=True,
        timeout=60.0,
    ) as client:
        result = execute_command(arguments, client)

    print(result)
