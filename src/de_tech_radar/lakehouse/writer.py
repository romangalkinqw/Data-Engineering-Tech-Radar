from collections.abc import Iterable
from typing import cast

import pyarrow as pa
from pyiceberg.table import Table, Transaction

from de_tech_radar.bronze.arrow import events_to_arrow_table
from de_tech_radar.bronze.gharchive import BronzeEvent
from de_tech_radar.silver.arrow import (
    silver_events_to_arrow_table,
)
from de_tech_radar.silver.gharchive import SilverEvent

AppendTarget = Table | Transaction


def append_bronze_events(
    table: AppendTarget,
    events: Iterable[BronzeEvent],
    *,
    snapshot_properties: dict[str, str] | None = None,
) -> int:
    """Append one Bronze event batch to an Iceberg table."""
    return _append_arrow_table(
        table,
        events_to_arrow_table(events),
        snapshot_properties,
    )


def append_silver_events(
    table: AppendTarget,
    events: Iterable[SilverEvent],
    *,
    snapshot_properties: dict[str, str] | None = None,
) -> int:
    """Append one Silver event batch to an Iceberg table."""
    return _append_arrow_table(
        table,
        silver_events_to_arrow_table(events),
        snapshot_properties,
    )


def _append_arrow_table(
    table: AppendTarget,
    arrow_table: pa.Table,
    snapshot_properties: dict[str, str] | None,
) -> int:
    row_count = cast(int, arrow_table.num_rows)

    if row_count == 0:
        return 0

    properties = snapshot_properties if snapshot_properties is not None else {}

    table.append(
        arrow_table,
        snapshot_properties=properties,
    )

    return row_count
