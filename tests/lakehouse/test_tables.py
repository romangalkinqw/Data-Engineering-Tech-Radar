from pathlib import Path

from de_tech_radar.lakehouse.catalog import open_local_catalog
from de_tech_radar.lakehouse.tables import (
    ensure_bronze_events_table,
    ensure_gold_daily_activity_table,
    ensure_silver_events_table,
)


def test_ensure_bronze_events_table_is_idempotent(
    tmp_path: Path,
) -> None:
    warehouse_path = tmp_path / "warehouse"

    with open_local_catalog(
        catalog_name="test",
        catalog_path=tmp_path / "catalog" / "iceberg.db",
        warehouse_path=warehouse_path,
    ) as catalog:
        first_table = ensure_bronze_events_table(catalog)
        second_table = ensure_bronze_events_table(catalog)

        assert first_table.name() == (
            "bronze",
            "gharchive_events",
        )
        assert second_table.name() == first_table.name()
        assert catalog.list_tables("bronze") == [("bronze", "gharchive_events")]

    metadata_files = list(warehouse_path.rglob("*.metadata.json"))

    assert len(metadata_files) == 1


def test_ensure_silver_events_table_is_partitioned_by_date(
    tmp_path: Path,
) -> None:
    warehouse_path = tmp_path / "warehouse"

    with open_local_catalog(
        catalog_name="test",
        catalog_path=tmp_path / "catalog" / "iceberg.db",
        warehouse_path=warehouse_path,
    ) as catalog:
        first_table = ensure_silver_events_table(catalog)
        second_table = ensure_silver_events_table(catalog)

        partition_fields = first_table.spec().fields

        assert first_table.name() == (
            "silver",
            "github_events",
        )
        assert second_table.name() == first_table.name()
        assert len(partition_fields) == 1
        assert partition_fields[0].name == "event_date"
        assert str(partition_fields[0].transform) == "identity"

    metadata_files = list(warehouse_path.rglob("*.metadata.json"))

    assert len(metadata_files) == 1


def test_ensure_gold_daily_activity_is_partitioned_by_date(
    tmp_path: Path,
) -> None:
    warehouse_path = tmp_path / "warehouse"

    with open_local_catalog(
        catalog_name="test",
        catalog_path=tmp_path / "catalog" / "iceberg.db",
        warehouse_path=warehouse_path,
    ) as catalog:
        first_table = ensure_gold_daily_activity_table(catalog)
        second_table = ensure_gold_daily_activity_table(catalog)

        partition_fields = first_table.spec().fields

        assert first_table.name() == (
            "gold",
            "daily_technology_activity",
        )
        assert second_table.name() == first_table.name()
        assert len(partition_fields) == 1
        assert partition_fields[0].name == "activity_date"
        assert str(partition_fields[0].transform) == "identity"

    metadata_files = list(warehouse_path.rglob("*.metadata.json"))

    assert len(metadata_files) == 1
