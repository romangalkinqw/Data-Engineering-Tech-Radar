from pyiceberg.schema import Schema
from pyiceberg.types import (
    BooleanType,
    DateType,
    LongType,
    NestedField,
    StringType,
    TimestamptzType,
)


def silver_event_iceberg_schema() -> Schema:
    """Return the Iceberg schema for Silver events."""
    return Schema(
        NestedField(
            1,
            "event_id",
            StringType(),
            required=True,
        ),
        NestedField(
            2,
            "event_type",
            StringType(),
            required=True,
        ),
        NestedField(
            3,
            "event_date",
            DateType(),
            required=True,
        ),
        NestedField(
            4,
            "created_at",
            TimestamptzType(),
            required=True,
        ),
        NestedField(5, "actor_id", LongType(), required=True),
        NestedField(
            6,
            "actor_login",
            StringType(),
            required=True,
        ),
        NestedField(7, "repo_id", LongType(), required=True),
        NestedField(
            8,
            "repo_name",
            StringType(),
            required=True,
        ),
        NestedField(9, "org_id", LongType(), required=False),
        NestedField(
            10,
            "org_login",
            StringType(),
            required=False,
        ),
        NestedField(
            11,
            "action",
            StringType(),
            required=False,
        ),
        NestedField(
            12,
            "commit_count",
            LongType(),
            required=False,
        ),
        NestedField(
            13,
            "is_merged",
            BooleanType(),
            required=False,
        ),
        NestedField(
            14,
            "archive_hour",
            TimestamptzType(),
            required=True,
        ),
        NestedField(
            15,
            "source_file",
            StringType(),
            required=True,
        ),
        NestedField(
            16,
            "source_line_number",
            LongType(),
            required=True,
        ),
    )
