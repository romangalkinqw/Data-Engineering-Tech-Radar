from datetime import date

from de_tech_radar.gold.daily_activity import (
    DailyTechnologyActivity,
)
from de_tech_radar.serving.postgres import (
    CREATE_DAILY_ACTIVITY_TABLE_SQL,
    UPSERT_DAILY_ACTIVITY_SQL,
    activity_to_parameters,
)


def test_serving_sql_uses_gold_business_key() -> None:
    assert "PRIMARY KEY (technology_id, activity_date)" in CREATE_DAILY_ACTIVITY_TABLE_SQL
    assert "ON CONFLICT (technology_id, activity_date)" in UPSERT_DAILY_ACTIVITY_SQL


def test_activity_to_parameters_preserves_values() -> None:
    activity = DailyTechnologyActivity(
        technology_id="apache-spark",
        activity_date=date(2025, 1, 2),
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

    assert activity_to_parameters(activity) == {
        "technology_id": "apache-spark",
        "activity_date": date(2025, 1, 2),
        "event_count": 10,
        "unique_actor_count": 4,
        "push_count": 2,
        "commit_count": 6,
        "pull_request_count": 1,
        "merged_pull_request_count": 1,
        "issue_count": 1,
        "star_count": 3,
        "fork_count": 1,
        "release_count": 0,
    }
