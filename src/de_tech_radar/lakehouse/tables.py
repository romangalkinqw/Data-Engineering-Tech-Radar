from pyiceberg.catalog import Catalog
from pyiceberg.table import Table

from de_tech_radar.bronze.arrow import bronze_event_schema

BRONZE_NAMESPACE = "bronze"
BRONZE_EVENTS_TABLE = "bronze.gharchive_events"


def ensure_bronze_events_table(
    catalog: Catalog,
) -> Table:
    """Create or load the Bronze GH Archive Iceberg table."""
    catalog.create_namespace_if_not_exists(BRONZE_NAMESPACE)

    if catalog.table_exists(BRONZE_EVENTS_TABLE):
        return catalog.load_table(BRONZE_EVENTS_TABLE)

    return catalog.create_table(
        BRONZE_EVENTS_TABLE,
        schema=bronze_event_schema(),
    )
