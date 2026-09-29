import gzip
import json
from datetime import UTC, datetime
from pathlib import Path

import httpx

from de_tech_radar.bronze.gharchive import BronzeEvent
from de_tech_radar.cli import execute_command, parse_args
from de_tech_radar.lakehouse.catalog import open_local_catalog
from de_tech_radar.lakehouse.tables import (
    ensure_bronze_events_table,
)
from de_tech_radar.lakehouse.writer import append_bronze_events


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

    assert isinstance(archive_path, Path)

    expected_path = (
        tmp_path
        / "gharchive"
        / "archive_date=2025-01-02"
        / "archive_hour=03"
        / "2025-01-02-3.json.gz"
    )

    assert archive_path == expected_path
    assert archive_path.read_bytes() == archive_bytes


def test_parse_load_gharchive_bronze_command() -> None:
    arguments = parse_args(
        [
            "load-gharchive-bronze",
            "--archive-path",
            "data/raw/2025-01-02-3.json.gz",
            "--hour",
            "2025-01-02T03:00:00Z",
        ]
    )

    assert arguments.command == "load-gharchive-bronze"
    assert arguments.archive_path == Path("data/raw/2025-01-02-3.json.gz")
    assert arguments.archive_hour == datetime(
        2025,
        1,
        2,
        3,
        tzinfo=UTC,
    )
    assert arguments.catalog_path == Path("data/lakehouse/catalog.db")
    assert arguments.warehouse_path == Path("data/lakehouse/warehouse")
    assert arguments.batch_size == 10_000


def test_execute_load_gharchive_bronze_command(
    tmp_path: Path,
) -> None:
    archive_path = tmp_path / "2025-01-02-3.json.gz"
    catalog_path = tmp_path / "catalog.db"
    warehouse_path = tmp_path / "warehouse"

    source_event = {
        "id": "123456",
        "type": "PushEvent",
        "created_at": "2025-01-02T03:15:00Z",
        "public": True,
        "actor": {
            "id": 10,
            "login": "alice",
        },
        "repo": {
            "id": 20,
            "name": "acme/project",
        },
        "payload": {
            "size": 1,
        },
    }

    with gzip.open(
        archive_path,
        "wt",
        encoding="utf-8",
        newline="\n",
    ) as stream:
        stream.write(json.dumps(source_event) + "\n")

    arguments = parse_args(
        [
            "load-gharchive-bronze",
            "--archive-path",
            str(archive_path),
            "--hour",
            "2025-01-02T03:00:00Z",
            "--catalog-path",
            str(catalog_path),
            "--warehouse-path",
            str(warehouse_path),
            "--batch-size",
            "100",
        ]
    )

    def reject_request(_: httpx.Request) -> httpx.Response:
        raise AssertionError("HTTP request must not be made")

    transport = httpx.MockTransport(reject_request)

    with httpx.Client(transport=transport) as client:
        written_rows = execute_command(arguments, client)

    assert written_rows == 1

    with open_local_catalog(
        catalog_name="local",
        catalog_path=catalog_path,
        warehouse_path=warehouse_path,
    ) as catalog:
        table = catalog.load_table("bronze.gharchive_events")

        assert table.scan().to_arrow().num_rows == 1


def test_parse_load_gharchive_silver_command() -> None:
    arguments = parse_args(
        [
            "load-gharchive-silver",
            "--source-file",
            "2025-01-02-3.json.gz",
        ]
    )

    assert arguments.command == "load-gharchive-silver"
    assert arguments.source_file == "2025-01-02-3.json.gz"
    assert arguments.catalog_path == Path("data/lakehouse/catalog.db")
    assert arguments.warehouse_path == Path("data/lakehouse/warehouse")


def test_execute_load_gharchive_silver_command(
    tmp_path: Path,
) -> None:
    catalog_path = tmp_path / "catalog.db"
    warehouse_path = tmp_path / "warehouse"
    source_file = "2025-01-02-3.json.gz"

    bronze_event = BronzeEvent(
        event_id="123456",
        event_type="PushEvent",
        created_at=datetime(
            2025,
            1,
            2,
            3,
            15,
            tzinfo=UTC,
        ),
        is_public=True,
        actor_id=10,
        actor_login="alice",
        repo_id=20,
        repo_name="acme/project",
        org_id=None,
        org_login=None,
        payload_json=json.dumps({"size": 2}),
        archive_hour=datetime(
            2025,
            1,
            2,
            3,
            tzinfo=UTC,
        ),
        source_file=source_file,
        source_line_number=1,
    )

    with open_local_catalog(
        catalog_name="local",
        catalog_path=catalog_path,
        warehouse_path=warehouse_path,
    ) as catalog:
        bronze_table = ensure_bronze_events_table(catalog)
        append_bronze_events(bronze_table, [bronze_event])

    arguments = parse_args(
        [
            "load-gharchive-silver",
            "--source-file",
            source_file,
            "--catalog-path",
            str(catalog_path),
            "--warehouse-path",
            str(warehouse_path),
        ]
    )

    def reject_request(_: httpx.Request) -> httpx.Response:
        raise AssertionError("HTTP request must not be made")

    transport = httpx.MockTransport(reject_request)

    with httpx.Client(transport=transport) as client:
        written_rows = execute_command(arguments, client)

    assert written_rows == 1

    with open_local_catalog(
        catalog_name="local",
        catalog_path=catalog_path,
        warehouse_path=warehouse_path,
    ) as catalog:
        silver_table = catalog.load_table("silver.github_events")
        rows = silver_table.scan().to_arrow().to_pylist()

    assert len(rows) == 1
    assert rows[0]["event_id"] == "123456"
    assert rows[0]["commit_count"] == 2
