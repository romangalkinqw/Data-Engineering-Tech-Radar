import json
from dataclasses import dataclass
from datetime import date, datetime
from typing import cast

from de_tech_radar.bronze.gharchive import BronzeEvent


@dataclass(frozen=True, slots=True)
class SilverEvent:
    event_id: str
    event_type: str
    event_date: date
    created_at: datetime
    actor_id: int
    actor_login: str
    repo_id: int
    repo_name: str
    org_id: int | None
    org_login: str | None
    action: str | None
    commit_count: int | None
    is_merged: bool | None
    archive_hour: datetime
    source_file: str
    source_line_number: int


def to_silver_event(
    event: BronzeEvent,
) -> SilverEvent:
    """Transform one Bronze event into a typed Silver event."""
    payload = cast(
        dict[str, object],
        json.loads(event.payload_json),
    )

    action_value = payload.get("action")
    if action_value is not None and not isinstance(
        action_value,
        str,
    ):
        raise ValueError("field 'payload.action' must be str")

    commit_count: int | None = None

    if event.event_type == "PushEvent":
        size = payload.get("size")

        if type(size) is not int:
            raise ValueError("field 'payload.size' must be int")

        commit_count = size

    is_merged: bool | None = None

    if event.event_type == "PullRequestEvent":
        pull_request_value = payload.get("pull_request")

        if not isinstance(pull_request_value, dict):
            raise ValueError("field 'payload.pull_request' must be dict")

        pull_request = cast(
            dict[str, object],
            pull_request_value,
        )
        merged = pull_request.get("merged")

        if type(merged) is not bool:
            raise ValueError("field 'payload.pull_request.merged' must be bool")

        is_merged = merged

    return SilverEvent(
        event_id=event.event_id,
        event_type=event.event_type,
        event_date=event.created_at.date(),
        created_at=event.created_at,
        actor_id=event.actor_id,
        actor_login=event.actor_login,
        repo_id=event.repo_id,
        repo_name=event.repo_name,
        org_id=event.org_id,
        org_login=event.org_login,
        action=action_value,
        commit_count=commit_count,
        is_merged=is_merged,
        archive_hour=event.archive_hour,
        source_file=event.source_file,
        source_line_number=event.source_line_number,
    )
