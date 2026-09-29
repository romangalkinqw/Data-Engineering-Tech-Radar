from pyiceberg.catalog import Catalog
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.table import Table
from pyiceberg.transforms import IdentityTransform

from de_tech_radar.bronze.arrow import bronze_event_schema
from de_tech_radar.silver.schema import (
    silver_event_iceberg_schema,
)

BRONZE_NAMESPACE = "bronze"
BRONZE_EVENTS_TABLE = "bronze.gharchive_events"

SILVER_NAMESPACE = "silver"
SILVER_EVENTS_TABLE = "silver.github_events"
SILVER_EVENT_DATE_PARTITION_FIELD_ID = 1000


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


def ensure_silver_events_table(
    catalog: Catalog,
) -> Table:
    """Create or load the partitioned Silver events table."""
    catalog.create_namespace_if_not_exists(SILVER_NAMESPACE)

    if catalog.table_exists(SILVER_EVENTS_TABLE):
        return catalog.load_table(SILVER_EVENTS_TABLE)

    iceberg_schema = silver_event_iceberg_schema()
    event_date_field = iceberg_schema.find_field("event_date")
    partition_spec = PartitionSpec(
        PartitionField(
            source_id=event_date_field.field_id,
            field_id=(SILVER_EVENT_DATE_PARTITION_FIELD_ID),
            transform=IdentityTransform(),
            name="event_date",
        )
    )

    return catalog.create_table(
        SILVER_EVENTS_TABLE,
        schema=iceberg_schema,
        partition_spec=partition_spec,
    )
