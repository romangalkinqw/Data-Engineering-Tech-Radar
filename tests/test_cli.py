import gzip
import json
from contextlib import nullcontext
from datetime import UTC, date, datetime
from pathlib import Path

import httpx
import psycopg
import pytest

import de_tech_radar.cli as cli_module
from de_tech_radar.bronze.gharchive import BronzeEvent
from de_tech_radar.cli import execute_command, parse_args
from de_tech_radar.lakehouse.catalog import open_local_catalog
from de_tech_radar.lakehouse.tables import (
    ensure_bronze_events_table,
    ensure_silver_events_table,
)
from de_tech_radar.lakehouse.writer import (
    append_bronze_events,
    append_silver_events,
)
from de_tech_radar.pipelines.daily import (
    DailyPipelineResult,
)
from de_tech_radar.silver.gharchive import SilverEvent


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


def test_parse_build_gold_daily_activity_command() -> None:
    arguments = parse_args(
        [
            "build-gold-daily-activity",
            "--date",
            "2025-01-02",
        ]
    )

    assert arguments.command == "build-gold-daily-activity"
    assert arguments.activity_date == date(
        2025,
        1,
        2,
    )
    assert arguments.technology_catalog_path == Path("config/technologies.toml")
    assert arguments.catalog_path == Path("data/lakehouse/catalog.db")
    assert arguments.warehouse_path == Path("data/lakehouse/warehouse")


def test_execute_build_gold_daily_activity_command(
    tmp_path: Path,
) -> None:
    target_date = date(2025, 1, 2)
    catalog_path = tmp_path / "catalog.db"
    warehouse_path = tmp_path / "warehouse"
    technology_catalog_path = tmp_path / "technologies.toml"
    technology_catalog_path.write_text(
        """
[[technologies]]
id = "apache-airflow"
name = "Apache Airflow"
category = "orchestration"
repositories = ["apache/airflow"]
""".strip(),
        encoding="utf-8",
    )

    event = SilverEvent(
        event_id="1",
        event_type="PushEvent",
        event_date=target_date,
        created_at=datetime(
            2025,
            1,
            2,
            3,
            15,
            tzinfo=UTC,
        ),
        actor_id=10,
        actor_login="alice",
        repo_id=20,
        repo_name="apache/airflow",
        org_id=None,
        org_login=None,
        action=None,
        commit_count=3,
        is_merged=None,
        archive_hour=datetime(
            2025,
            1,
            2,
            3,
            tzinfo=UTC,
        ),
        source_file="source.json.gz",
        source_line_number=1,
    )

    with open_local_catalog(
        catalog_name="local",
        catalog_path=catalog_path,
        warehouse_path=warehouse_path,
    ) as catalog:
        silver_table = ensure_silver_events_table(catalog)
        append_silver_events(silver_table, [event])

    arguments = parse_args(
        [
            "build-gold-daily-activity",
            "--date",
            target_date.isoformat(),
            "--technology-catalog",
            str(technology_catalog_path),
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
        rows = catalog.load_table("gold.daily_technology_activity").scan().to_arrow().to_pylist()

    assert len(rows) == 1
    assert rows[0]["technology_id"] == "apache-airflow"
    assert rows[0]["event_count"] == 1
    assert rows[0]["commit_count"] == 3


def test_parse_run_gharchive_day_command() -> None:
    arguments = parse_args(
        [
            "run-gharchive-day",
            "--date",
            "2025-01-02",
            "--download-concurrency",
            "3",
        ]
    )

    assert arguments.command == "run-gharchive-day"
    assert arguments.activity_date == date(
        2025,
        1,
        2,
    )
    assert arguments.raw_root == Path("data/raw")
    assert arguments.technology_catalog_path == Path("config/technologies.toml")
    assert arguments.catalog_path == Path("data/lakehouse/catalog.db")
    assert arguments.warehouse_path == Path("data/lakehouse/warehouse")
    assert arguments.download_concurrency == 3


def test_execute_run_gharchive_day_command(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target_date = date(2025, 1, 2)
    raw_root = tmp_path / "raw"
    catalog_path = tmp_path / "catalog.db"
    warehouse_path = tmp_path / "warehouse"
    technology_catalog_path = tmp_path / "technologies.toml"
    technology_catalog_path.write_text(
        """
[[technologies]]
id = "apache-airflow"
name = "Apache Airflow"
category = "orchestration"
repositories = ["apache/airflow"]
""".strip(),
        encoding="utf-8",
    )

    expected_result = DailyPipelineResult(
        archive_count=24,
        bronze_rows=24,
        silver_rows=24,
        gold_rows=1,
    )

    def fake_run_daily_pipeline(
        **arguments: object,
    ) -> DailyPipelineResult:
        assert arguments["activity_date"] == target_date
        assert arguments["raw_root"] == raw_root
        assert arguments["max_download_concurrency"] == 3

        return expected_result

    monkeypatch.setattr(
        cli_module,
        "run_daily_pipeline",
        fake_run_daily_pipeline,
        raising=False,
    )

    arguments = parse_args(
        [
            "run-gharchive-day",
            "--date",
            target_date.isoformat(),
            "--download-concurrency",
            "3",
            "--raw-root",
            str(raw_root),
            "--technology-catalog",
            str(technology_catalog_path),
            "--catalog-path",
            str(catalog_path),
            "--warehouse-path",
            str(warehouse_path),
        ]
    )

    def reject_request(_: httpx.Request) -> httpx.Response:
        raise AssertionError("mocked daily pipeline must not use HTTP")

    transport = httpx.MockTransport(reject_request)

    with httpx.Client(transport=transport) as client:
        result = execute_command(arguments, client)

    assert result == expected_result


def test_parse_publish_gold_postgres_command() -> None:
    arguments = parse_args(
        [
            "publish-gold-postgres",
            "--date",
            "2025-01-02",
        ]
    )

    assert arguments.command == "publish-gold-postgres"
    assert arguments.activity_date == date(
        2025,
        1,
        2,
    )
    assert arguments.catalog_path == Path("data/lakehouse/catalog.db")
    assert arguments.warehouse_path == Path("data/lakehouse/warehouse")


def test_execute_publish_gold_postgres_command(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target_date = date(2025, 1, 2)
    catalog_path = tmp_path / "catalog.db"
    warehouse_path = tmp_path / "warehouse"
    fake_connection = object()
    postgres_dsn = "postgresql://test"

    def fake_connect(
        conninfo: str,
        *,
        connect_timeout: int,
    ) -> nullcontext[object]:
        assert conninfo == postgres_dsn
        assert connect_timeout == 5

        return nullcontext(fake_connection)

    def fake_publish_gold_date_to_postgres(
        **arguments: object,
    ) -> int:
        assert arguments["connection"] is fake_connection
        assert arguments["activity_date"] == target_date

        return 2

    monkeypatch.setenv(
        "DE_TECH_RADAR_POSTGRES_DSN",
        postgres_dsn,
    )
    monkeypatch.setattr(
        psycopg,
        "connect",
        fake_connect,
    )
    monkeypatch.setattr(
        cli_module,
        "publish_gold_date_to_postgres",
        fake_publish_gold_date_to_postgres,
        raising=False,
    )

    arguments = parse_args(
        [
            "publish-gold-postgres",
            "--date",
            target_date.isoformat(),
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
        result = execute_command(arguments, client)

    assert result == 2
