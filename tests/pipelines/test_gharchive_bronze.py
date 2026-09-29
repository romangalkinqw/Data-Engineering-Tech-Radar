import gzip
import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from de_tech_radar.bronze.gharchive import EventParseError
from de_tech_radar.lakehouse.catalog import open_local_catalog
from de_tech_radar.lakehouse.tables import ensure_bronze_events_table
from de_tech_radar.pipelines.gharchive_bronze import (
    load_archive_to_bronze,
)


def _source_event(event_id: str) -> dict[str, object]:
    return {
        "id": event_id,
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


def test_load_archive_to_bronze_writes_bounded_batches(
    tmp_path: Path,
) -> None:
    archive_hour = datetime(2025, 1, 2, 3, tzinfo=UTC)
    archive_path = tmp_path / "2025-01-02-3.json.gz"
    warehouse_path = tmp_path / "warehouse"

    with gzip.open(
        archive_path,
        "wt",
        encoding="utf-8",
        newline="\n",
    ) as stream:
        for event_id in ("1", "2", "3"):
            stream.write(json.dumps(_source_event(event_id)) + "\n")

    with open_local_catalog(
        catalog_name="test",
        catalog_path=tmp_path / "catalog" / "iceberg.db",
        warehouse_path=warehouse_path,
    ) as catalog:
        table = ensure_bronze_events_table(catalog)

        written_rows = load_archive_to_bronze(
            archive_path=archive_path,
            archive_hour=archive_hour,
            table=table,
            batch_size=2,
        )

        result = (
            catalog.load_table("bronze.gharchive_events")
            .scan(selected_fields=("event_id",))
            .to_arrow()
        )

        assert written_rows == 3
        assert sorted(result.column("event_id").to_pylist()) == ["1", "2", "3"]

    assert len(list(warehouse_path.rglob("*.parquet"))) == 2


def test_load_archive_to_bronze_does_not_publish_partial_archive(
    tmp_path: Path,
) -> None:
    archive_hour = datetime(2025, 1, 2, 3, tzinfo=UTC)
    archive_path = tmp_path / "2025-01-02-3.json.gz"

    invalid_event = _source_event("3")
    invalid_event["id"] = 3

    with gzip.open(
        archive_path,
        "wt",
        encoding="utf-8",
        newline="\n",
    ) as stream:
        stream.write(json.dumps(_source_event("1")) + "\n")
        stream.write(json.dumps(_source_event("2")) + "\n")
        stream.write(json.dumps(invalid_event) + "\n")

    with open_local_catalog(
        catalog_name="test",
        catalog_path=tmp_path / "catalog" / "iceberg.db",
        warehouse_path=tmp_path / "warehouse",
    ) as catalog:
        table = ensure_bronze_events_table(catalog)

        with pytest.raises(
            EventParseError,
            match="field 'id' must be str",
        ):
            load_archive_to_bronze(
                archive_path=archive_path,
                archive_hour=archive_hour,
                table=table,
                batch_size=2,
            )

        reloaded_table = catalog.load_table("bronze.gharchive_events")

        assert reloaded_table.current_snapshot() is None


def test_load_archive_to_bronze_skips_committed_archive(
    tmp_path: Path,
) -> None:
    archive_hour = datetime(2025, 1, 2, 3, tzinfo=UTC)
    archive_path = tmp_path / "2025-01-02-3.json.gz"

    with gzip.open(
        archive_path,
        "wt",
        encoding="utf-8",
        newline="\n",
    ) as stream:
        stream.write(json.dumps(_source_event("1")) + "\n")
        stream.write(json.dumps(_source_event("2")) + "\n")

    with open_local_catalog(
        catalog_name="test",
        catalog_path=tmp_path / "catalog" / "iceberg.db",
        warehouse_path=tmp_path / "warehouse",
    ) as catalog:
        table = ensure_bronze_events_table(catalog)

        first_written_rows = load_archive_to_bronze(
            archive_path=archive_path,
            archive_hour=archive_hour,
            table=table,
            batch_size=2,
        )
        second_written_rows = load_archive_to_bronze(
            archive_path=archive_path,
            archive_hour=archive_hour,
            table=table,
            batch_size=2,
        )

        reloaded_table = catalog.load_table("bronze.gharchive_events")
        result = reloaded_table.scan(selected_fields=("event_id",)).to_arrow()

        snapshot_sources = [
            snapshot.summary.get("de-tech-radar.source-file")
            for snapshot in reloaded_table.snapshots()
            if snapshot.summary is not None
        ]

        assert first_written_rows == 2
        assert second_written_rows == 0
        assert result.num_rows == 2
        assert archive_path.name in snapshot_sources
