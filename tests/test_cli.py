import gzip
from datetime import UTC, datetime
from pathlib import Path

import httpx

from de_tech_radar.cli import execute_command, parse_args


def test_parse_ingest_gharchive_command() -> None:
    arguments = parse_args(
        [
            "ingest-gharchive",
            "--hour",
            "2025-01-02T03:00:00Z",
        ]
    )

    assert arguments.command == "ingest-gharchive"
    assert arguments.archive_hour == datetime(2025, 1, 2, 3, tzinfo=UTC)
    assert arguments.raw_root == Path("data/raw")


def test_execute_ingest_gharchive_command(tmp_path: Path) -> None:
    archive_bytes = gzip.compress(b'{"type":"PushEvent"}\n')
    arguments = parse_args(
        [
            "ingest-gharchive",
            "--hour",
            "2025-01-02T03:00:00Z",
            "--raw-root",
            str(tmp_path),
        ]
    )

    def handle_request(_: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            stream=httpx.ByteStream(archive_bytes),
        )

    transport = httpx.MockTransport(handle_request)

    with httpx.Client(transport=transport) as client:
        archive_path = execute_command(arguments, client)

    expected_path = (
        tmp_path
        / "gharchive"
        / "archive_date=2025-01-02"
        / "archive_hour=03"
        / "2025-01-02-3.json.gz"
    )

    assert archive_path == expected_path
    assert archive_path.read_bytes() == archive_bytes
