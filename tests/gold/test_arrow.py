from datetime import date

import pyarrow as pa

from de_tech_radar.gold.arrow import (
    daily_activities_to_arrow_table,
    daily_activity_arrow_schema,
)
from de_tech_radar.gold.daily_activity import (
    DailyTechnologyActivity,
)


def test_daily_activity_schema_matches_storage_contract() -> None:
    expected = pa.schema(
        [
            pa.field(
                "technology_id",
                pa.large_string(),
                nullable=False,
            ),
            pa.field(
                "activity_date",
                pa.date32(),
                nullable=False,
            ),
            pa.field(
                "event_count",
                pa.int64(),
                nullable=False,
            ),
            pa.field(
                "unique_actor_count",
                pa.int64(),
                nullable=False,
            ),
            pa.field(
                "push_count",
                pa.int64(),
                nullable=False,
            ),
            pa.field(
                "commit_count",
                pa.int64(),
                nullable=False,
            ),
            pa.field(
                "pull_request_count",
                pa.int64(),
                nullable=False,
            ),
            pa.field(
                "merged_pull_request_count",
                pa.int64(),
                nullable=False,
            ),
            pa.field(
                "issue_count",
                pa.int64(),
                nullable=False,
            ),
            pa.field(
                "star_count",
                pa.int64(),
                nullable=False,
            ),
            pa.field(
                "fork_count",
                pa.int64(),
                nullable=False,
            ),
            pa.field(
                "release_count",
                pa.int64(),
                nullable=False,
            ),
        ]
    )

    assert daily_activity_arrow_schema() == expected


def test_daily_activities_to_arrow_table_preserves_values() -> None:
    activity = DailyTechnologyActivity(
        technology_id="apache-airflow",
        activity_date=date(2025, 1, 2),
        event_count=10,
        unique_actor_count=7,
        push_count=3,
        commit_count=8,
        pull_request_count=2,
        merged_pull_request_count=1,
        issue_count=1,
        star_count=2,
        fork_count=1,
        release_count=1,
    )

    table = daily_activities_to_arrow_table([activity])

    assert table.schema == daily_activity_arrow_schema()
    assert table.to_pylist() == [
        {
            "technology_id": "apache-airflow",
            "activity_date": date(2025, 1, 2),
            "event_count": 10,
            "unique_actor_count": 7,
            "push_count": 3,
            "commit_count": 8,
            "pull_request_count": 2,
            "merged_pull_request_count": 1,
            "issue_count": 1,
            "star_count": 2,
            "fork_count": 1,
            "release_count": 1,
        }
    ]
