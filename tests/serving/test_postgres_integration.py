import os
from dataclasses import replace
from datetime import date

import psycopg
import pytest

from de_tech_radar.gold.daily_activity import (
    DailyTechnologyActivity,
)
from de_tech_radar.serving.postgres import (
    publish_daily_activities,
    replace_daily_activity_date,
)

TEST_DSN = os.getenv("TEST_POSTGRES_DSN")


@pytest.mark.skipif(
    TEST_DSN is None,
    reason="TEST_POSTGRES_DSN is not configured",
)
def test_publish_daily_activities_is_idempotent() -> None:
    assert TEST_DSN is not None

    activity = DailyTechnologyActivity(
        technology_id="integration-test",
        activity_date=date(2099, 1, 1),
        event_count=10,
        unique_actor_count=4,
        push_count=2,
        commit_count=6,
        pull_request_count=1,
        merged_pull_request_count=1,
        issue_count=1,
        star_count=3,
        fork_count=1,
        release_count=0,
    )

    updated_activity = replace(
        activity,
        event_count=11,
    )

    with psycopg.connect(
        TEST_DSN,
        connect_timeout=5,
    ) as connection:
        first_count = publish_daily_activities(
            connection,
            [activity],
        )
        second_count = publish_daily_activities(
            connection,
            [updated_activity],
        )

        row = connection.execute(
            """
            SELECT COUNT(*), MAX(event_count)
            FROM analytics.daily_technology_activity
            WHERE technology_id = %s
              AND activity_date = %s
            """,
            (
                activity.technology_id,
                activity.activity_date,
            ),
        ).fetchone()

        connection.execute(
            """
            DELETE FROM analytics.daily_technology_activity
            WHERE technology_id = %s
              AND activity_date = %s
            """,
            (
                activity.technology_id,
                activity.activity_date,
            ),
        )

    assert first_count == 1
    assert second_count == 1
    assert row == (1, 11)


@pytest.mark.skipif(
    TEST_DSN is None,
    reason="TEST_POSTGRES_DSN is not configured",
)
def test_replace_daily_activity_date_removes_stale_rows() -> None:
    assert TEST_DSN is not None

    activity_date = date(2099, 1, 2)
    spark = DailyTechnologyActivity(
        technology_id="apache-spark-test",
        activity_date=activity_date,
        event_count=10,
        unique_actor_count=4,
        push_count=2,
        commit_count=6,
        pull_request_count=1,
        merged_pull_request_count=1,
        issue_count=1,
        star_count=3,
        fork_count=1,
        release_count=0,
    )
    kafka = replace(
        spark,
        technology_id="apache-kafka-test",
    )
    updated_spark = replace(
        spark,
        event_count=20,
    )

    with psycopg.connect(
        TEST_DSN,
        connect_timeout=5,
    ) as connection:
        publish_daily_activities(
            connection,
            [spark, kafka],
        )

        written_rows = replace_daily_activity_date(
            connection,
            activity_date,
            [updated_spark],
        )

        rows = connection.execute(
            """
            SELECT technology_id, event_count
            FROM analytics.daily_technology_activity
            WHERE activity_date = %s
            ORDER BY technology_id
            """,
            (activity_date,),
        ).fetchall()

        connection.execute(
            """
            DELETE FROM analytics.daily_technology_activity
            WHERE activity_date = %s
            """,
            (activity_date,),
        )

    assert written_rows == 1
    assert rows == [
        ("apache-spark-test", 20),
    ]
