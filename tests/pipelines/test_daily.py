import gzip
import json
from datetime import UTC, date, datetime
from pathlib import Path

import httpx

from de_tech_radar.lakehouse.catalog import open_local_catalog
from de_tech_radar.lakehouse.tables import (
    ensure_bronze_events_table,
    ensure_gold_daily_activity_table,
    ensure_silver_events_table,
)
from de_tech_radar.pipelines.daily import (
    DailyPipelineResult,
    build_archive_hours,
    run_archive_hours,
)
from de_tech_radar.technologies.catalog import Technology


def test_build_archive_hours_returns_full_utc_day() -> None:
    archive_hours = build_archive_hours(date(2025, 1, 2))

    assert len(archive_hours) == 24
    assert archive_hours[0] == datetime(
        2025,
        1,
        2,
        0,
        tzinfo=UTC,
    )
    assert archive_hours[-1] == datetime(
        2025,
        1,
        2,
        23,
        tzinfo=UTC,
    )
    assert all(archive_hour.tzinfo is UTC for archive_hour in archive_hours)


def test_run_archive_hours_executes_complete_pipeline(
    tmp_path: Path,
) -> None:
    archive_hour = datetime(
        2025,
        1,
        2,
        3,
        tzinfo=UTC,
    )
    source_event = {
        "id": "1",
        "type": "PushEvent",
        "created_at": "2025-01-02T03:15:00Z",
        "public": True,
        "actor": {
            "id": 10,
            "login": "alice",
        },
        "repo": {
            "id": 20,
            "name": "apache/airflow",
        },
        "payload": {
            "size": 2,
        },
    }
    archive_bytes = gzip.compress((json.dumps(source_event) + "\n").encode("utf-8"))

    def handle_request(
        request: httpx.Request,
    ) -> httpx.Response:
        assert str(request.url) == ("https://data.gharchive.org/2025-01-02-3.json.gz")

        return httpx.Response(
            200,
            stream=httpx.ByteStream(archive_bytes),
        )

    airflow = Technology(
        technology_id="apache-airflow",
        name="Apache Airflow",
        category="orchestration",
        repositories=("apache/airflow",),
    )
    transport = httpx.MockTransport(handle_request)

    with (
        open_local_catalog(
            catalog_name="test",
            catalog_path=tmp_path / "catalog.db",
            warehouse_path=tmp_path / "warehouse",
        ) as catalog,
        httpx.Client(transport=transport) as client,
    ):
        bronze_table = ensure_bronze_events_table(catalog)
        silver_table = ensure_silver_events_table(catalog)
        gold_table = ensure_gold_daily_activity_table(catalog)

        result = run_archive_hours(
            archive_hours=[archive_hour],
            raw_root=tmp_path / "raw",
            bronze_table=bronze_table,
            silver_table=silver_table,
            gold_table=gold_table,
            technologies=[airflow],
            client=client,
        )

        retry_result = run_archive_hours(
            archive_hours=[archive_hour],
            raw_root=tmp_path / "raw",
            bronze_table=bronze_table,
            silver_table=silver_table,
            gold_table=gold_table,
            technologies=[airflow],
            client=client,
        )

        assert result == DailyPipelineResult(
            archive_count=1,
            bronze_rows=1,
            silver_rows=1,
            gold_rows=1,
        )
        assert retry_result == DailyPipelineResult(
            archive_count=1,
            bronze_rows=0,
            silver_rows=0,
            gold_rows=1,
        )
        assert bronze_table.scan().count() == 1
        assert silver_table.scan().count() == 1
        assert gold_table.scan().count() == 1
