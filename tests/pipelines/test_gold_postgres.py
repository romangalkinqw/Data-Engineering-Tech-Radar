import os
from datetime import date
from pathlib import Path

import psycopg
import pytest

from de_tech_radar.gold.daily_activity import (
    DailyTechnologyActivity,
)
from de_tech_radar.lakehouse.catalog import open_local_catalog
from de_tech_radar.lakehouse.tables import (
    ensure_gold_daily_activity_table,
)
from de_tech_radar.lakehouse.writer import (
    replace_gold_daily_activity,
)
from de_tech_radar.pipelines.gold_postgres import (
    publish_gold_date_to_postgres,
)

TEST_DSN = os.getenv("TEST_POSTGRES_DSN")


@pytest.mark.skipif(
    TEST_DSN is None,
    reason="TEST_POSTGRES_DSN is not configured",
)
def test_publish_gold_date_to_postgres_is_idempotent(
    tmp_path: Path,
) -> None:
    assert TEST_DSN is not None

    target_date = date(2099, 2, 1)
    activity = DailyTechnologyActivity(
        technology_id="pipeline-integration-test",
        activity_date=target_date,
        event_count=12,
        unique_actor_count=5,
        push_count=3,
        commit_count=8,
        pull_request_count=2,
        merged_pull_request_count=1,
        issue_count=1,
        star_count=4,
        fork_count=2,
        release_count=1,
    )

    with open_local_catalog(
        catalog_name="test",
        catalog_path=tmp_path / "catalog.db",
        warehouse_path=tmp_path / "warehouse",
    ) as catalog:
        gold_table = ensure_gold_daily_activity_table(catalog)
        replace_gold_daily_activity(
            gold_table,
            target_date,
            [activity],
        )

        with psycopg.connect(
            TEST_DSN,
            connect_timeout=5,
        ) as connection:
            first_count = publish_gold_date_to_postgres(
                gold_table=gold_table,
                connection=connection,
                activity_date=target_date,
            )
            second_count = publish_gold_date_to_postgres(
                gold_table=gold_table,
                connection=connection,
                activity_date=target_date,
            )

            rows = connection.execute(
                """
                SELECT technology_id, event_count
                FROM analytics.daily_technology_activity
                WHERE activity_date = %s
                """,
                (target_date,),
            ).fetchall()

            connection.execute(
                """
                DELETE FROM analytics.daily_technology_activity
                WHERE activity_date = %s
                """,
                (target_date,),
            )

    assert first_count == 1
    assert second_count == 1
    assert rows == [
        ("pipeline-integration-test", 12),
    ]
