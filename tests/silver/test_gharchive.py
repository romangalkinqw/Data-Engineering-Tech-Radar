from datetime import UTC, date, datetime

from de_tech_radar.bronze.gharchive import BronzeEvent
from de_tech_radar.silver.gharchive import to_silver_event


def _bronze_event(
    *,
    event_type: str,
    payload_json: str,
) -> BronzeEvent:
    return BronzeEvent(
        event_id="123456",
        event_type=event_type,
        created_at=datetime(
            2025,
            1,
            2,
            3,
            15,
            tzinfo=UTC,
        ),
        is_public=True,
        actor_id=10,
        actor_login="alice",
        repo_id=20,
        repo_name="acme/project",
        org_id=None,
        org_login=None,
        payload_json=payload_json,
        archive_hour=datetime(
            2025,
            1,
            2,
            3,
            tzinfo=UTC,
        ),
        source_file="2025-01-02-3.json.gz",
        source_line_number=42,
    )


def test_to_silver_event_extracts_push_metrics() -> None:
    bronze_event = _bronze_event(
        event_type="PushEvent",
        payload_json=('{"size":3,"ref":"refs/heads/main"}'),
    )

    silver_event = to_silver_event(bronze_event)

    assert silver_event.event_id == "123456"
    assert silver_event.event_type == "PushEvent"
    assert silver_event.event_date == date(2025, 1, 2)
    assert silver_event.repo_name == "acme/project"
    assert silver_event.action is None
    assert silver_event.commit_count == 3
    assert silver_event.is_merged is None


def test_to_silver_event_extracts_pull_request_metrics() -> None:
    bronze_event = _bronze_event(
        event_type="PullRequestEvent",
        payload_json=('{"action":"closed","number":17,"pull_request":{"merged":true}}'),
    )

    silver_event = to_silver_event(bronze_event)

    assert silver_event.action == "closed"
    assert silver_event.commit_count is None
    assert silver_event.is_merged is True
