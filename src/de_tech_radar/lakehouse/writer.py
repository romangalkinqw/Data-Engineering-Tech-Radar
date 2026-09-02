from collections.abc import Iterable
from typing import cast

from pyiceberg.table import Table

from de_tech_radar.bronze.arrow import events_to_arrow_table
from de_tech_radar.bronze.gharchive import BronzeEvent


def append_bronze_events(
    table: Table,
    events: Iterable[BronzeEvent],
) -> int:
    """Append one Bronze event batch to an Iceberg table."""
    arrow_table = events_to_arrow_table(events)
    row_count = cast(int, arrow_table.num_rows)

    if row_count == 0:
        return 0

    table.append(arrow_table)

    return row_count
