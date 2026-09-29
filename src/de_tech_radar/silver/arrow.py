from collections.abc import Iterable
from dataclasses import asdict

import pyarrow as pa
from pyiceberg.io.pyarrow import schema_to_pyarrow

from de_tech_radar.silver.gharchive import SilverEvent
from de_tech_radar.silver.schema import (
    silver_event_iceberg_schema,
)


def silver_event_schema() -> pa.Schema:
    """Return the Arrow representation of the Silver schema."""
    return schema_to_pyarrow(
        silver_event_iceberg_schema(),
        include_field_ids=False,
    )


def silver_events_to_arrow_table(
    events: Iterable[SilverEvent],
) -> pa.Table:
    """Convert one Silver event batch into an Arrow table."""
    records = [asdict(event) for event in events]

    return pa.Table.from_pylist(
        records,
        schema=silver_event_schema(),
    )
