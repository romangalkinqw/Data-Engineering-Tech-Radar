from datetime import UTC, date, datetime

from de_tech_radar.gold.daily_activity import (
    DailyTechnologyActivity,
    aggregate_daily_activity,
)
from de_tech_radar.silver.gharchive import SilverEvent
from de_tech_radar.technologies.catalog import (
    Technology,
    build_repository_index,
)


def _silver_event(
    *,
    event_id: str,
    event_type: str,
    actor_id: int,
    repo_name: str,
    commit_count: int | None = None,
    is_merged: bool | None = None,
    action: str | None = None,
) -> SilverEvent:
    return SilverEvent(
        event_id=event_id,
        event_type=event_type,
        event_date=date(2025, 1, 2),
        created_at=datetime(
            2025,
            1,
            2,
            3,
            15,
            tzinfo=UTC,
        ),
        actor_id=actor_id,
        actor_login=f"user-{actor_id}",
        repo_id=20,
        repo_name=repo_name,
        org_id=None,
        org_login=None,
        action=action,
        commit_count=commit_count,
        is_merged=is_merged,
        archive_hour=datetime(
            2025,
            1,
            2,
            3,
            tzinfo=UTC,
        ),
        source_file="2025-01-02-3.json.gz",
        source_line_number=int(event_id),
    )


def test_aggregate_daily_activity_for_technology() -> None:
    airflow = Technology(
        technology_id="apache-airflow",
        name="Apache Airflow",
        category="orchestration",
        repositories=("apache/airflow",),
    )
    repository_index = build_repository_index([airflow])

    events = [
        _silver_event(
            event_id="1",
            event_type="PushEvent",
            actor_id=10,
            repo_name="apache/airflow",
            commit_count=3,
        ),
        _silver_event(
            event_id="2",
            event_type="WatchEvent",
            actor_id=11,
            repo_name="Apache/Airflow",
            action="started",
        ),
    ]

    result = aggregate_daily_activity(
        events,
        repository_index,
    )

    assert result == [
        DailyTechnologyActivity(
            technology_id="apache-airflow",
            activity_date=date(2025, 1, 2),
            event_count=2,
            unique_actor_count=2,
            push_count=1,
            commit_count=3,
            pull_request_count=0,
            merged_pull_request_count=0,
            issue_count=0,
            star_count=1,
            fork_count=0,
            release_count=0,
        )
    ]


def test_aggregate_daily_activity_counts_event_metrics() -> None:
    airflow = Technology(
        technology_id="apache-airflow",
        name="Apache Airflow",
        category="orchestration",
        repositories=("apache/airflow",),
    )
    repository_index = build_repository_index([airflow])

    events = [
        _silver_event(
            event_id="1",
            event_type="PullRequestEvent",
            actor_id=10,
            repo_name="apache/airflow",
            action="closed",
            is_merged=True,
        ),
        _silver_event(
            event_id="2",
            event_type="IssuesEvent",
            actor_id=10,
            repo_name="apache/airflow",
            action="opened",
        ),
        _silver_event(
            event_id="3",
            event_type="ForkEvent",
            actor_id=11,
            repo_name="apache/airflow",
        ),
        _silver_event(
            event_id="4",
            event_type="ReleaseEvent",
            actor_id=12,
            repo_name="apache/airflow",
            action="published",
        ),
    ]

    result = aggregate_daily_activity(
        events,
        repository_index,
    )

    assert result == [
        DailyTechnologyActivity(
            technology_id="apache-airflow",
            activity_date=date(2025, 1, 2),
            event_count=4,
            unique_actor_count=3,
            push_count=0,
            commit_count=0,
            pull_request_count=1,
            merged_pull_request_count=1,
            issue_count=1,
            star_count=0,
            fork_count=1,
            release_count=1,
        )
    ]


def test_aggregate_daily_activity_ignores_unknown_repositories() -> None:
    airflow = Technology(
        technology_id="apache-airflow",
        name="Apache Airflow",
        category="orchestration",
        repositories=("apache/airflow",),
    )
    repository_index = build_repository_index([airflow])

    events = [
        _silver_event(
            event_id="1",
            event_type="PushEvent",
            actor_id=10,
            repo_name="apache/airflow",
            commit_count=2,
        ),
        _silver_event(
            event_id="2",
            event_type="PushEvent",
            actor_id=11,
            repo_name="example/unknown",
            commit_count=100,
        ),
    ]

    result = aggregate_daily_activity(
        events,
        repository_index,
    )

    assert len(result) == 1
    assert result[0].technology_id == "apache-airflow"
    assert result[0].event_count == 1
    assert result[0].commit_count == 2
