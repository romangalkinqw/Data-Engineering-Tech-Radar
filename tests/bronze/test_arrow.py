from datetime import UTC, datetime

import pyarrow as pa

from de_tech_radar.bronze.arrow import (
    bronze_event_schema,
    events_to_arrow_table,
)
from de_tech_radar.bronze.gharchive import BronzeEvent


def test_bronze_event_schema_matches_storage_contract() -> None:
    expected = pa.schema(
        [
            pa.field("event_id", pa.string(), nullable=False),
            pa.field("event_type", pa.string(), nullable=False),
            pa.field(
                "created_at",
                pa.timestamp("us", tz="UTC"),
                nullable=False,
            ),
            pa.field("is_public", pa.bool_(), nullable=False),
            pa.field("actor_id", pa.int64(), nullable=False),
            pa.field("actor_login", pa.string(), nullable=False),
            pa.field("repo_id", pa.int64(), nullable=False),
            pa.field("repo_name", pa.string(), nullable=False),
            pa.field("org_id", pa.int64(), nullable=True),
            pa.field("org_login", pa.string(), nullable=True),
            pa.field("payload_json", pa.string(), nullable=False),
            pa.field(
                "archive_hour",
                pa.timestamp("us", tz="UTC"),
                nullable=False,
            ),
            pa.field("source_file", pa.string(), nullable=False),
            pa.field("source_line_number", pa.int64(), nullable=False),
        ]
    )

    assert bronze_event_schema() == expected


def test_events_to_arrow_table_preserves_event_values() -> None:
    created_at = datetime(2025, 1, 2, 3, 15, tzinfo=UTC)
    archive_hour = datetime(2025, 1, 2, 3, tzinfo=UTC)

    event = BronzeEvent(
        event_id="123456",
        event_type="PushEvent",
        created_at=created_at,
        is_public=True,
        actor_id=10,
        actor_login="alice",
        repo_id=20,
        repo_name="acme/project",
        org_id=None,
        org_login=None,
        payload_json='{"size":2}',
        archive_hour=archive_hour,
        source_file="2025-01-02-3.json.gz",
        source_line_number=42,
    )

    table = events_to_arrow_table([event])

    assert table.schema == bronze_event_schema()
    assert table.to_pylist() == [
        {
            "event_id": "123456",
            "event_type": "PushEvent",
            "created_at": created_at,
            "is_public": True,
            "actor_id": 10,
            "actor_login": "alice",
            "repo_id": 20,
            "repo_name": "acme/project",
            "org_id": None,
            "org_login": None,
            "payload_json": '{"size":2}',
            "archive_hour": archive_hour,
            "source_file": "2025-01-02-3.json.gz",
            "source_line_number": 42,
        }
    ]
