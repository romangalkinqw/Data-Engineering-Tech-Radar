from collections.abc import Iterable
from dataclasses import asdict

import pyarrow as pa

from de_tech_radar.bronze.gharchive import BronzeEvent


def bronze_event_schema() -> pa.Schema:
    """Return the physical Arrow schema for Bronze GH Archive events."""
    return pa.schema(
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


def events_to_arrow_table(
    events: Iterable[BronzeEvent],
) -> pa.Table:
    """Convert one batch of Bronze events into an Arrow table."""
    records = [asdict(event) for event in events]

    return pa.Table.from_pylist(
        records,
        schema=bronze_event_schema(),
    )
