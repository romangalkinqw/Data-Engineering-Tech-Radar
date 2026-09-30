from collections.abc import Iterable
from datetime import date
from typing import Any

import psycopg

from de_tech_radar.gold.daily_activity import (
    DailyTechnologyActivity,
)

CREATE_ANALYTICS_SCHEMA_SQL = """
CREATE SCHEMA IF NOT EXISTS analytics
"""

CREATE_DAILY_ACTIVITY_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS analytics.daily_technology_activity (
    technology_id TEXT NOT NULL,
    activity_date DATE NOT NULL,
    event_count BIGINT NOT NULL,
    unique_actor_count BIGINT NOT NULL,
    push_count BIGINT NOT NULL,
    commit_count BIGINT NOT NULL,
    pull_request_count BIGINT NOT NULL,
    merged_pull_request_count BIGINT NOT NULL,
    issue_count BIGINT NOT NULL,
    star_count BIGINT NOT NULL,
    fork_count BIGINT NOT NULL,
    release_count BIGINT NOT NULL,
    published_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (technology_id, activity_date)
)
"""

UPSERT_DAILY_ACTIVITY_SQL = """
INSERT INTO analytics.daily_technology_activity (
    technology_id,
    activity_date,
    event_count,
    unique_actor_count,
    push_count,
    commit_count,
    pull_request_count,
    merged_pull_request_count,
    issue_count,
    star_count,
    fork_count,
    release_count
)
VALUES (
    %(technology_id)s,
    %(activity_date)s,
    %(event_count)s,
    %(unique_actor_count)s,
    %(push_count)s,
    %(commit_count)s,
    %(pull_request_count)s,
    %(merged_pull_request_count)s,
    %(issue_count)s,
    %(star_count)s,
    %(fork_count)s,
    %(release_count)s
)
ON CONFLICT (technology_id, activity_date)
DO UPDATE SET
    event_count = EXCLUDED.event_count,
    unique_actor_count = EXCLUDED.unique_actor_count,
    push_count = EXCLUDED.push_count,
    commit_count = EXCLUDED.commit_count,
    pull_request_count = EXCLUDED.pull_request_count,
    merged_pull_request_count =
        EXCLUDED.merged_pull_request_count,
    issue_count = EXCLUDED.issue_count,
    star_count = EXCLUDED.star_count,
    fork_count = EXCLUDED.fork_count,
    release_count = EXCLUDED.release_count,
    published_at = CURRENT_TIMESTAMP
"""


DELETE_DAILY_ACTIVITY_DATE_SQL = """
DELETE FROM analytics.daily_technology_activity
WHERE activity_date = %s
"""


def activity_to_parameters(
    activity: DailyTechnologyActivity,
) -> dict[str, object]:
    """Convert one Gold row into PostgreSQL parameters."""

    return {
        "technology_id": activity.technology_id,
        "activity_date": activity.activity_date,
        "event_count": activity.event_count,
        "unique_actor_count": activity.unique_actor_count,
        "push_count": activity.push_count,
        "commit_count": activity.commit_count,
        "pull_request_count": (activity.pull_request_count),
        "merged_pull_request_count": (activity.merged_pull_request_count),
        "issue_count": activity.issue_count,
        "star_count": activity.star_count,
        "fork_count": activity.fork_count,
        "release_count": activity.release_count,
    }


def publish_daily_activities(
    connection: psycopg.Connection[tuple[Any, ...]],
    activities: Iterable[DailyTechnologyActivity],
) -> int:
    """Atomically upsert Gold activity rows into PostgreSQL."""

    activity_rows = tuple(activities)
    parameters = [activity_to_parameters(activity) for activity in activity_rows]

    with (
        connection.transaction(),
        connection.cursor() as cursor,
    ):
        cursor.execute(CREATE_ANALYTICS_SCHEMA_SQL)
        cursor.execute(CREATE_DAILY_ACTIVITY_TABLE_SQL)

        if parameters:
            cursor.executemany(
                UPSERT_DAILY_ACTIVITY_SQL,
                parameters,
            )

    return len(activity_rows)


def replace_daily_activity_date(
    connection: psycopg.Connection[tuple[Any, ...]],
    activity_date: date,
    activities: Iterable[DailyTechnologyActivity],
) -> int:
    """Atomically replace one PostgreSQL serving date."""

    activity_rows = tuple(activities)

    if any(activity.activity_date != activity_date for activity in activity_rows):
        raise ValueError(f"all serving rows must match activity_date={activity_date.isoformat()}")

    parameters = [activity_to_parameters(activity) for activity in activity_rows]

    with (
        connection.transaction(),
        connection.cursor() as cursor,
    ):
        cursor.execute(CREATE_ANALYTICS_SCHEMA_SQL)
        cursor.execute(CREATE_DAILY_ACTIVITY_TABLE_SQL)
        cursor.execute(
            DELETE_DAILY_ACTIVITY_DATE_SQL,
            (activity_date,),
        )

        if parameters:
            cursor.executemany(
                UPSERT_DAILY_ACTIVITY_SQL,
                parameters,
            )

    return len(activity_rows)
