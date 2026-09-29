from collections.abc import Iterable
from typing import cast

from pyiceberg.table import Table, Transaction

from de_tech_radar.bronze.arrow import events_to_arrow_table
from de_tech_radar.bronze.gharchive import BronzeEvent


def append_bronze_events(
    table: Table | Transaction,
    events: Iterable[BronzeEvent],
    *,
    snapshot_properties: dict[str, str] | None = None,
) -> int:
    """Append one Bronze event batch to an Iceberg table."""
    arrow_table = events_to_arrow_table(events)
    row_count = cast(int, arrow_table.num_rows)

    if row_count == 0:
        return 0

    properties = snapshot_properties if snapshot_properties is not None else {}

    table.append(
        arrow_table,
        snapshot_properties=properties,
    )

    return row_count
