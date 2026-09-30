from collections.abc import Iterable, Iterator
from datetime import date
from typing import Any, cast

import pyarrow as pa
from pyiceberg.expressions import EqualTo, Reference
from pyiceberg.expressions.literals import literal
from pyiceberg.table import Table

from de_tech_radar.gold.daily_activity import (
    aggregate_daily_activity,
)
from de_tech_radar.lakehouse.writer import (
    replace_gold_daily_activity,
)
from de_tech_radar.silver.gharchive import SilverEvent
from de_tech_radar.technologies.catalog import (
    Technology,
    build_repository_index,
)


def load_silver_date_to_gold(
    *,
    silver_table: Table,
    gold_table: Table,
    activity_date: date,
    technologies: Iterable[Technology],
) -> int:
    """Aggregate one Silver date into the Gold table."""
    repository_index = build_repository_index(technologies)
    silver_table.refresh()

    reader = silver_table.scan(
        row_filter=EqualTo(
            term=Reference("event_date"),
            value=literal(activity_date),
        )
    ).to_arrow_batch_reader()

    try:
        activities = aggregate_daily_activity(
            _iter_silver_events(reader),
            repository_index,
        )
    finally:
        reader.close()

    return replace_gold_daily_activity(
        gold_table,
        activity_date,
        activities,
    )


def _iter_silver_events(
    batches: Iterable[pa.RecordBatch],
) -> Iterator[SilverEvent]:
    for batch in batches:
        records = cast(
            list[dict[str, Any]],
            batch.to_pylist(),
        )

        for record in records:
            yield SilverEvent(**record)
