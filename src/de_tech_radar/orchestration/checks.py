from collections import Counter
from collections.abc import Iterable
from datetime import date
from typing import Any, cast

import dagster as dg
from pyiceberg.expressions import EqualTo, Reference
from pyiceberg.expressions.literals import literal
from pyiceberg.table import Table

from de_tech_radar.gold.daily_activity import (
    DailyTechnologyActivity,
)
from de_tech_radar.lakehouse.catalog import open_local_catalog
from de_tech_radar.lakehouse.tables import (
    ensure_gold_daily_activity_table,
)
from de_tech_radar.orchestration.assets import (
    daily_technology_activity,
)
from de_tech_radar.orchestration.resources import RadarPaths

GoldActivityKey = tuple[str, date]


def evaluate_unique_gold_keys(
    keys: Iterable[GoldActivityKey],
) -> dg.AssetCheckResult:
    """Check uniqueness of technology and activity date pairs."""

    materialized_keys = tuple(keys)
    key_counts = Counter(materialized_keys)

    duplicate_key_count = sum(count > 1 for count in key_counts.values())

    return dg.AssetCheckResult(
        passed=duplicate_key_count == 0,
        metadata={
            "row_count": len(materialized_keys),
            "duplicate_key_count": duplicate_key_count,
        },
    )


def evaluate_valid_gold_metrics(
    activities: Iterable[DailyTechnologyActivity],
) -> dg.AssetCheckResult:
    """Check that Gold activity metrics are non-negative."""

    materialized_activities = tuple(activities)

    invalid_row_count = sum(_has_invalid_metrics(activity) for activity in materialized_activities)

    return dg.AssetCheckResult(
        passed=invalid_row_count == 0,
        metadata={
            "row_count": len(materialized_activities),
            "invalid_row_count": invalid_row_count,
        },
    )


def _has_negative_metric(
    activity: DailyTechnologyActivity,
) -> bool:
    metrics = (
        activity.event_count,
        activity.unique_actor_count,
        activity.push_count,
        activity.commit_count,
        activity.pull_request_count,
        activity.merged_pull_request_count,
        activity.issue_count,
        activity.star_count,
        activity.fork_count,
        activity.release_count,
    )

    return any(metric < 0 for metric in metrics)


def load_gold_activity_keys(
    *,
    table: Table,
    activity_date: date,
) -> tuple[GoldActivityKey, ...]:
    """Read Gold business keys for one date partition."""

    table.refresh()

    records = cast(
        list[dict[str, Any]],
        table.scan(
            row_filter=EqualTo(
                term=Reference("activity_date"),
                value=literal(activity_date),
            ),
            selected_fields=(
                "technology_id",
                "activity_date",
            ),
        )
        .to_arrow()
        .to_pylist(),
    )

    return tuple(
        (
            cast(str, record["technology_id"]),
            cast(date, record["activity_date"]),
        )
        for record in records
    )


def load_gold_activities(
    *,
    table: Table,
    activity_date: date,
) -> tuple[DailyTechnologyActivity, ...]:
    """Read complete Gold rows for one date partition."""

    table.refresh()

    records = cast(
        list[dict[str, Any]],
        table.scan(
            row_filter=EqualTo(
                term=Reference("activity_date"),
                value=literal(activity_date),
            )
        )
        .to_arrow()
        .to_pylist(),
    )

    return tuple(DailyTechnologyActivity(**record) for record in records)


def _has_invalid_metrics(
    activity: DailyTechnologyActivity,
) -> bool:
    metrics = (
        activity.event_count,
        activity.unique_actor_count,
        activity.push_count,
        activity.commit_count,
        activity.pull_request_count,
        activity.merged_pull_request_count,
        activity.issue_count,
        activity.star_count,
        activity.fork_count,
        activity.release_count,
    )

    if any(metric < 0 for metric in metrics):
        return True

    event_subtype_counts = (
        activity.push_count,
        activity.pull_request_count,
        activity.issue_count,
        activity.star_count,
        activity.fork_count,
        activity.release_count,
    )

    return (
        activity.unique_actor_count > activity.event_count
        or activity.merged_pull_request_count > activity.pull_request_count
        or any(count > activity.event_count for count in event_subtype_counts)
    )


@dg.asset_check(
    asset=daily_technology_activity,
    name="unique_gold_keys",
    description=("Each technology must have at most one Gold row per activity date."),
    blocking=True,
)
def unique_gold_keys(
    context: dg.AssetCheckExecutionContext,
    paths: RadarPaths,
) -> dg.AssetCheckResult:
    """Check uniqueness in the selected Gold partition."""

    activity_date = date.fromisoformat(context.partition_key)

    with open_local_catalog(
        catalog_name="local",
        catalog_path=paths.resolve(paths.catalog_path),
        warehouse_path=paths.resolve(paths.warehouse_path),
    ) as catalog:
        table = ensure_gold_daily_activity_table(catalog)

        return evaluate_unique_gold_keys(
            load_gold_activity_keys(
                table=table,
                activity_date=activity_date,
            )
        )


@dg.asset_check(
    asset=daily_technology_activity,
    name="valid_gold_metrics",
    description=("Gold activity counters must be non-negative and internally consistent."),
    blocking=True,
)
def valid_gold_metrics(
    context: dg.AssetCheckExecutionContext,
    paths: RadarPaths,
) -> dg.AssetCheckResult:
    """Validate metrics in the selected Gold partition."""

    activity_date = date.fromisoformat(context.partition_key)

    with open_local_catalog(
        catalog_name="local",
        catalog_path=paths.resolve(paths.catalog_path),
        warehouse_path=paths.resolve(paths.warehouse_path),
    ) as catalog:
        table = ensure_gold_daily_activity_table(catalog)

        return evaluate_valid_gold_metrics(
            load_gold_activities(
                table=table,
                activity_date=activity_date,
            )
        )
