from collections.abc import Iterable
from dataclasses import asdict

import pyarrow as pa
from pyiceberg.io.pyarrow import schema_to_pyarrow

from de_tech_radar.gold.daily_activity import (
    DailyTechnologyActivity,
)
from de_tech_radar.gold.schema import (
    daily_activity_iceberg_schema,
)


def daily_activity_arrow_schema() -> pa.Schema:
    """Return the Arrow representation of the Gold schema."""
    return schema_to_pyarrow(
        daily_activity_iceberg_schema(),
        include_field_ids=False,
    )


def daily_activities_to_arrow_table(
    activities: Iterable[DailyTechnologyActivity],
) -> pa.Table:
    """Convert Gold activity records into an Arrow table."""
    rows = [asdict(activity) for activity in activities]

    return pa.Table.from_pylist(
        rows,
        schema=daily_activity_arrow_schema(),
    )
