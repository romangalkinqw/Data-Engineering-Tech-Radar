import argparse
from collections.abc import Sequence
from datetime import datetime
from pathlib import Path
from typing import cast

import httpx

from de_tech_radar.ingestion.gharchive import download_archive


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

    return parser.parse_args(argv)


def execute_command(
    arguments: argparse.Namespace,
    client: httpx.Client,
) -> Path:
    """Execute a parsed CLI command."""
    if arguments.command == "ingest-gharchive":
        return download_archive(
            archive_hour=cast(datetime, arguments.archive_hour),
            raw_root=cast(Path, arguments.raw_root),
            client=client,
        )

    raise ValueError(f"unsupported command: {arguments.command}")


def _parse_archive_hour(value: str) -> datetime:
    try:
        return datetime.fromisoformat(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError(f"invalid ISO 8601 datetime: {value}") from error


def main() -> None:
    """Run the command-line interface."""
    arguments = parse_args()

    with httpx.Client(
        follow_redirects=True,
        timeout=60.0,
    ) as client:
        archive_path = execute_command(arguments, client)

    print(archive_path)
