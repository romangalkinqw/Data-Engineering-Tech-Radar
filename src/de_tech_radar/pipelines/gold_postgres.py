from collections.abc import Iterable, Iterator
from datetime import date
from typing import Any, cast

import psycopg
import pyarrow as pa
from pyiceberg.expressions import EqualTo, Reference
from pyiceberg.expressions.literals import literal
from pyiceberg.table import Table

from de_tech_radar.gold.daily_activity import (
    DailyTechnologyActivity,
)
from de_tech_radar.serving.postgres import (
    replace_daily_activity_date,
)


def publish_gold_date_to_postgres(
    *,
    gold_table: Table,
    connection: psycopg.Connection[tuple[Any, ...]],
    activity_date: date,
) -> int:
    """Publish one Iceberg Gold date into PostgreSQL."""

    gold_table.refresh()
    reader = gold_table.scan(
        row_filter=EqualTo(
            term=Reference("activity_date"),
            value=literal(activity_date),
        )
    ).to_arrow_batch_reader()

    try:
        activities = tuple(_iter_daily_activities(reader))
    finally:
        reader.close()

    return replace_daily_activity_date(
        connection,
        activity_date,
        activities,
    )


def _iter_daily_activities(
    batches: Iterable[pa.RecordBatch],
) -> Iterator[DailyTechnologyActivity]:
    for batch in batches:
        records = cast(
            list[dict[str, Any]],
            batch.to_pylist(),
        )

        for record in records:
            yield DailyTechnologyActivity(**record)
