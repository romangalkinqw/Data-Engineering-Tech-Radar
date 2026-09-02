from pathlib import Path

from de_tech_radar.bronze.arrow import bronze_event_schema
from de_tech_radar.lakehouse.catalog import open_local_catalog


def test_open_local_catalog_can_create_iceberg_table(
    tmp_path: Path,
) -> None:
    catalog_path = tmp_path / "catalog" / "iceberg.db"
    warehouse_path = tmp_path / "warehouse"

    with open_local_catalog(
        catalog_name="test",
        catalog_path=catalog_path,
        warehouse_path=warehouse_path,
    ) as catalog:
        catalog.create_namespace("bronze")

        table = catalog.create_table(
            "bronze.gharchive_events",
            schema=bronze_event_schema(),
        )

        assert catalog.table_exists("bronze.gharchive_events")
        assert table.name() == ("bronze", "gharchive_events")
        assert table.location().startswith(warehouse_path.resolve().as_posix())

    assert catalog_path.is_file()
