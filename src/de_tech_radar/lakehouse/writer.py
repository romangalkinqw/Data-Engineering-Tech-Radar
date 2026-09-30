from collections.abc import Iterable
from datetime import date
from typing import cast

import pyarrow as pa
from pyiceberg.expressions import EqualTo, Reference
from pyiceberg.expressions.literals import literal
from pyiceberg.table import Table, Transaction

from de_tech_radar.bronze.arrow import events_to_arrow_table
from de_tech_radar.bronze.gharchive import BronzeEvent
from de_tech_radar.gold.arrow import (
    daily_activities_to_arrow_table,
)
from de_tech_radar.gold.daily_activity import (
    DailyTechnologyActivity,
)
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


def replace_gold_daily_activity(
    table: Table,
    activity_date: date,
    activities: Iterable[DailyTechnologyActivity],
) -> int:
    """Atomically replace one Gold date partition."""
    activity_rows = tuple(activities)

    if any(activity.activity_date != activity_date for activity in activity_rows):
        raise ValueError(f"all Gold rows must match activity_date={activity_date.isoformat()}")

    arrow_table = daily_activities_to_arrow_table(activity_rows)
    row_count = cast(int, arrow_table.num_rows)

    partition_filter = EqualTo(
        term=Reference("activity_date"),
        value=literal(activity_date),
    )
    partition_exists = (
        table.current_snapshot() is not None and table.scan(row_filter=partition_filter).count() > 0
    )

    if not partition_exists:
        if row_count > 0:
            table.append(arrow_table)

        return row_count

    table.overwrite(
        arrow_table,
        overwrite_filter=partition_filter,
    )

    return row_count


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
