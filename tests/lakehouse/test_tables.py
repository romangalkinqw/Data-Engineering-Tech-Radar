from pathlib import Path

from de_tech_radar.lakehouse.catalog import open_local_catalog
from de_tech_radar.lakehouse.tables import ensure_bronze_events_table


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
