import gzip
import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from de_tech_radar.bronze.gharchive import (
    EventParseError,
    iter_archive_events,
    parse_event_line,
)


def test_parse_event_line_maps_common_fields() -> None:
    source_event = {
        "id": "123456",
        "type": "PushEvent",
        "created_at": "2025-01-02T03:15:00Z",
        "public": True,
        "actor": {
            "id": 10,
            "login": "alice",
        },
        "repo": {
            "id": 20,
            "name": "acme/project",
        },
        "org": {
            "id": 30,
            "login": "acme",
        },
        "payload": {
            "size": 1,
            "ref": "refs/heads/main",
        },
    }

    record = parse_event_line(
        json.dumps(source_event),
        source_file="2025-01-02-3.json.gz",
        source_line_number=42,
        archive_hour=datetime(2025, 1, 2, 3, tzinfo=UTC),
    )

    assert record.event_id == "123456"
    assert record.event_type == "PushEvent"
    assert record.created_at == datetime(2025, 1, 2, 3, 15, tzinfo=UTC)
    assert record.is_public is True
    assert record.actor_id == 10
    assert record.actor_login == "alice"
    assert record.repo_id == 20
    assert record.repo_name == "acme/project"
    assert record.org_id == 30
    assert record.org_login == "acme"
    assert json.loads(record.payload_json) == source_event["payload"]
    assert record.archive_hour == datetime(2025, 1, 2, 3, tzinfo=UTC)
    assert record.source_file == "2025-01-02-3.json.gz"
    assert record.source_line_number == 42


def test_parse_event_line_allows_missing_org() -> None:
    source_event = {
        "id": "654321",
        "type": "PublicEvent",
        "created_at": "2025-01-02T03:20:00Z",
        "public": True,
        "actor": {
            "id": 10,
            "login": "alice",
        },
        "repo": {
            "id": 20,
            "name": "alice/project",
        },
        "payload": {},
    }

    record = parse_event_line(
        json.dumps(source_event),
        source_file="2025-01-02-3.json.gz",
        source_line_number=43,
        archive_hour=datetime(2025, 1, 2, 3, tzinfo=UTC),
    )

    assert record.org_id is None
    assert record.org_login is None


def test_iter_archive_events_streams_gzip_lines(tmp_path: Path) -> None:
    archive_path = tmp_path / "2025-01-02-3.json.gz"

    first_event = {
        "id": "1",
        "type": "PushEvent",
        "created_at": "2025-01-02T03:15:00Z",
        "public": True,
        "actor": {"id": 10, "login": "alice"},
        "repo": {"id": 20, "name": "acme/project"},
        "payload": {"size": 1},
    }
    second_event = {
        **first_event,
        "id": "2",
        "type": "WatchEvent",
        "payload": {"action": "started"},
    }

    with gzip.open(
        archive_path,
        "wt",
        encoding="utf-8",
        newline="\n",
    ) as stream:
        stream.write(json.dumps(first_event) + "\n")
        stream.write(json.dumps(second_event) + "\n")

    records = list(
        iter_archive_events(
            archive_path,
            archive_hour=datetime(2025, 1, 2, 3, tzinfo=UTC),
        )
    )

    assert [record.event_id for record in records] == ["1", "2"]
    assert [record.event_type for record in records] == [
        "PushEvent",
        "WatchEvent",
    ]
    assert [record.source_line_number for record in records] == [1, 2]
    assert all(record.source_file == archive_path.as_posix() for record in records)


def test_parse_event_line_rejects_invalid_event_id_type() -> None:
    source_event = {
        "id": 123456,
        "type": "PushEvent",
        "created_at": "2025-01-02T03:15:00Z",
        "public": True,
        "actor": {"id": 10, "login": "alice"},
        "repo": {"id": 20, "name": "acme/project"},
        "payload": {},
    }

    with pytest.raises(
        EventParseError,
        match=(
            r"2025-01-02-3\.json\.gz:7: "
            r"field 'id' must be str"
        ),
    ):
        parse_event_line(
            json.dumps(source_event),
            source_file="2025-01-02-3.json.gz",
            source_line_number=7,
            archive_hour=datetime(2025, 1, 2, 3, tzinfo=UTC),
        )
