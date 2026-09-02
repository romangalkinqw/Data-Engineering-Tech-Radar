from datetime import UTC, datetime
from pathlib import Path

from de_tech_radar.bronze.gharchive import BronzeEvent
from de_tech_radar.lakehouse.catalog import open_local_catalog
from de_tech_radar.lakehouse.tables import ensure_bronze_events_table
from de_tech_radar.lakehouse.writer import append_bronze_events


def test_append_bronze_events_writes_readable_snapshot(
    tmp_path: Path,
) -> None:
    warehouse_path = tmp_path / "warehouse"

    event = BronzeEvent(
        event_id="123456",
        event_type="PushEvent",
        created_at=datetime(2025, 1, 2, 3, 15, tzinfo=UTC),
        is_public=True,
        actor_id=10,
        actor_login="alice",
        repo_id=20,
        repo_name="acme/project",
        org_id=None,
        org_login=None,
        payload_json='{"size":2}',
        archive_hour=datetime(2025, 1, 2, 3, tzinfo=UTC),
        source_file="2025-01-02-3.json.gz",
        source_line_number=42,
    )

    with open_local_catalog(
        catalog_name="test",
        catalog_path=tmp_path / "catalog" / "iceberg.db",
        warehouse_path=warehouse_path,
    ) as catalog:
        table = ensure_bronze_events_table(catalog)

        written_rows = append_bronze_events(
            table,
            [event],
        )

        reloaded_table = catalog.load_table("bronze.gharchive_events")
        result = reloaded_table.scan(
            selected_fields=(
                "event_id",
                "event_type",
                "org_id",
            )
        ).to_arrow()

        assert written_rows == 1
        assert reloaded_table.current_snapshot() is not None
        assert result.to_pylist() == [
            {
                "event_id": "123456",
                "event_type": "PushEvent",
                "org_id": None,
            }
        ]

    assert len(list(warehouse_path.rglob("*.parquet"))) == 1


def test_append_bronze_events_skips_empty_batch(
    tmp_path: Path,
) -> None:
    warehouse_path = tmp_path / "warehouse"

    with open_local_catalog(
        catalog_name="test",
        catalog_path=tmp_path / "catalog" / "iceberg.db",
        warehouse_path=warehouse_path,
    ) as catalog:
        table = ensure_bronze_events_table(catalog)

        written_rows = append_bronze_events(table, [])

        assert written_rows == 0
        assert table.current_snapshot() is None

    assert list(warehouse_path.rglob("*.parquet")) == []
