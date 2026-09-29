import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from de_tech_radar.bronze.gharchive import BronzeEvent
from de_tech_radar.lakehouse.catalog import open_local_catalog
from de_tech_radar.lakehouse.tables import (
    ensure_bronze_events_table,
    ensure_silver_events_table,
)
from de_tech_radar.lakehouse.writer import append_bronze_events
from de_tech_radar.pipelines.bronze_silver import (
    load_bronze_source_to_silver,
)


def _bronze_event(
    *,
    event_id: str,
    event_type: str,
    payload: dict[str, object],
    source_file: str,
) -> BronzeEvent:
    return BronzeEvent(
        event_id=event_id,
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
        payload_json=json.dumps(payload),
        archive_hour=datetime(
            2025,
            1,
            2,
            3,
            tzinfo=UTC,
        ),
        source_file=source_file,
        source_line_number=int(event_id),
    )


def test_load_bronze_source_to_silver_transforms_events(
    tmp_path: Path,
) -> None:
    source_file = "data/raw/2025-01-02-3.json.gz"

    bronze_events = [
        _bronze_event(
            event_id="1",
            event_type="PushEvent",
            payload={"size": 2},
            source_file=source_file,
        ),
        _bronze_event(
            event_id="2",
            event_type="PullRequestEvent",
            payload={
                "action": "closed",
                "pull_request": {
                    "merged": True,
                },
            },
            source_file=source_file,
        ),
    ]

    with open_local_catalog(
        catalog_name="test",
        catalog_path=tmp_path / "catalog.db",
        warehouse_path=tmp_path / "warehouse",
    ) as catalog:
        bronze_table = ensure_bronze_events_table(catalog)
        silver_table = ensure_silver_events_table(catalog)

        append_bronze_events(
            bronze_table,
            bronze_events,
        )

        written_rows = load_bronze_source_to_silver(
            bronze_table=bronze_table,
            silver_table=silver_table,
            source_file=source_file,
        )

        result = (
            catalog.load_table("silver.github_events")
            .scan(
                selected_fields=(
                    "event_id",
                    "action",
                    "commit_count",
                    "is_merged",
                )
            )
            .to_arrow()
        )

        rows = sorted(
            result.to_pylist(),
            key=lambda row: row["event_id"],
        )

        assert written_rows == 2
        assert rows == [
            {
                "event_id": "1",
                "action": None,
                "commit_count": 2,
                "is_merged": None,
            },
            {
                "event_id": "2",
                "action": "closed",
                "commit_count": None,
                "is_merged": True,
            },
        ]


def test_load_bronze_source_to_silver_is_atomic(
    tmp_path: Path,
) -> None:
    source_file = "data/raw/2025-01-02-3.json.gz"

    valid_event = _bronze_event(
        event_id="1",
        event_type="PushEvent",
        payload={"size": 2},
        source_file=source_file,
    )
    invalid_event = _bronze_event(
        event_id="2",
        event_type="PushEvent",
        payload={"size": "not-an-integer"},
        source_file=source_file,
    )

    with open_local_catalog(
        catalog_name="test",
        catalog_path=tmp_path / "catalog.db",
        warehouse_path=tmp_path / "warehouse",
    ) as catalog:
        bronze_table = ensure_bronze_events_table(catalog)
        silver_table = ensure_silver_events_table(catalog)

        # Два append создают два отдельных Bronze data-файла.
        # Так ошибка возникает уже после начала обработки источника.
        append_bronze_events(bronze_table, [valid_event])
        append_bronze_events(bronze_table, [invalid_event])

        with pytest.raises(
            ValueError,
            match="field 'payload.size' must be int",
        ):
            load_bronze_source_to_silver(
                bronze_table=bronze_table,
                silver_table=silver_table,
                source_file=source_file,
            )

        reloaded_silver_table = catalog.load_table("silver.github_events")

        assert reloaded_silver_table.current_snapshot() is None


def test_load_bronze_source_to_silver_is_idempotent(
    tmp_path: Path,
) -> None:
    source_file = "data/raw/2025-01-02-3.json.gz"
    bronze_event = _bronze_event(
        event_id="1",
        event_type="PushEvent",
        payload={"size": 2},
        source_file=source_file,
    )

    with open_local_catalog(
        catalog_name="test",
        catalog_path=tmp_path / "catalog.db",
        warehouse_path=tmp_path / "warehouse",
    ) as catalog:
        bronze_table = ensure_bronze_events_table(catalog)
        silver_table = ensure_silver_events_table(catalog)

        append_bronze_events(bronze_table, [bronze_event])

        first_written_rows = load_bronze_source_to_silver(
            bronze_table=bronze_table,
            silver_table=silver_table,
            source_file=source_file,
        )
        second_written_rows = load_bronze_source_to_silver(
            bronze_table=bronze_table,
            silver_table=silver_table,
            source_file=source_file,
        )

        reloaded_silver_table = catalog.load_table("silver.github_events")
        rows = reloaded_silver_table.scan().to_arrow().to_pylist()

        assert first_written_rows == 1
        assert second_written_rows == 0
        assert len(rows) == 1


def test_load_bronze_source_to_silver_rejects_missing_source(
    tmp_path: Path,
) -> None:
    source_file = "missing.json.gz"

    with open_local_catalog(
        catalog_name="test",
        catalog_path=tmp_path / "catalog.db",
        warehouse_path=tmp_path / "warehouse",
    ) as catalog:
        bronze_table = ensure_bronze_events_table(catalog)
        silver_table = ensure_silver_events_table(catalog)

        with pytest.raises(
            ValueError,
            match="Bronze source file not found: missing.json.gz",
        ):
            load_bronze_source_to_silver(
                bronze_table=bronze_table,
                silver_table=silver_table,
                source_file=source_file,
            )

        reloaded_silver_table = catalog.load_table("silver.github_events")

        assert reloaded_silver_table.current_snapshot() is None
