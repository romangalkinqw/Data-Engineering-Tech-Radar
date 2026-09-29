from datetime import UTC, date, datetime

import pyarrow as pa

from de_tech_radar.silver.arrow import (
    silver_event_schema,
    silver_events_to_arrow_table,
)
from de_tech_radar.silver.gharchive import SilverEvent


def test_silver_event_schema_matches_storage_contract() -> None:
    expected = pa.schema(
        [
            pa.field("event_id", pa.large_string(), nullable=False),
            pa.field("event_type", pa.large_string(), nullable=False),
            pa.field("event_date", pa.date32(), nullable=False),
            pa.field(
                "created_at",
                pa.timestamp("us", tz="UTC"),
                nullable=False,
            ),
            pa.field("actor_id", pa.int64(), nullable=False),
            pa.field("actor_login", pa.large_string(), nullable=False),
            pa.field("repo_id", pa.int64(), nullable=False),
            pa.field("repo_name", pa.large_string(), nullable=False),
            pa.field("org_id", pa.int64(), nullable=True),
            pa.field("org_login", pa.large_string(), nullable=True),
            pa.field("action", pa.large_string(), nullable=True),
            pa.field("commit_count", pa.int64(), nullable=True),
            pa.field("is_merged", pa.bool_(), nullable=True),
            pa.field(
                "archive_hour",
                pa.timestamp("us", tz="UTC"),
                nullable=False,
            ),
            pa.field("source_file", pa.large_string(), nullable=False),
            pa.field(
                "source_line_number",
                pa.int64(),
                nullable=False,
            ),
        ]
    )

    assert silver_event_schema() == expected


def test_silver_events_to_arrow_table_preserves_types() -> None:
    event = SilverEvent(
        event_id="123456",
        event_type="PushEvent",
        event_date=date(2025, 1, 2),
        created_at=datetime(
            2025,
            1,
            2,
            3,
            15,
            tzinfo=UTC,
        ),
        actor_id=10,
        actor_login="alice",
        repo_id=20,
        repo_name="acme/project",
        org_id=None,
        org_login=None,
        action=None,
        commit_count=3,
        is_merged=None,
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

    table = silver_events_to_arrow_table([event])

    assert table.schema == silver_event_schema()
    assert table.num_rows == 1
    assert table.column("event_date").to_pylist() == [date(2025, 1, 2)]
    assert table.column("commit_count").to_pylist() == [3]
    assert table.column("is_merged").to_pylist() == [None]
