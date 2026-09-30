from datetime import UTC, date, datetime
from pathlib import Path

from de_tech_radar.lakehouse.catalog import open_local_catalog
from de_tech_radar.lakehouse.tables import (
    ensure_gold_daily_activity_table,
    ensure_silver_events_table,
)
from de_tech_radar.lakehouse.writer import append_silver_events
from de_tech_radar.pipelines.silver_gold import (
    load_silver_date_to_gold,
)
from de_tech_radar.silver.gharchive import SilverEvent
from de_tech_radar.technologies.catalog import Technology


def _silver_event(
    *,
    event_id: str,
    event_type: str,
    actor_id: int,
    repo_name: str,
    activity_date: date,
    commit_count: int | None = None,
    action: str | None = None,
) -> SilverEvent:
    created_at = datetime(
        activity_date.year,
        activity_date.month,
        activity_date.day,
        3,
        15,
        tzinfo=UTC,
    )

    return SilverEvent(
        event_id=event_id,
        event_type=event_type,
        event_date=activity_date,
        created_at=created_at,
        actor_id=actor_id,
        actor_login=f"user-{actor_id}",
        repo_id=20,
        repo_name=repo_name,
        org_id=None,
        org_login=None,
        action=action,
        commit_count=commit_count,
        is_merged=None,
        archive_hour=created_at.replace(
            minute=0,
            second=0,
        ),
        source_file="source.json.gz",
        source_line_number=int(event_id),
    )


def test_load_silver_date_to_gold_aggregates_tracked_technology(
    tmp_path: Path,
) -> None:
    target_date = date(2025, 1, 2)
    airflow = Technology(
        technology_id="apache-airflow",
        name="Apache Airflow",
        category="orchestration",
        repositories=("apache/airflow",),
    )
    events = [
        _silver_event(
            event_id="1",
            event_type="PushEvent",
            actor_id=10,
            repo_name="apache/airflow",
            activity_date=target_date,
            commit_count=3,
        ),
        _silver_event(
            event_id="2",
            event_type="WatchEvent",
            actor_id=11,
            repo_name="Apache/Airflow",
            activity_date=target_date,
            action="started",
        ),
        _silver_event(
            event_id="3",
            event_type="PushEvent",
            actor_id=12,
            repo_name="example/unknown",
            activity_date=target_date,
            commit_count=100,
        ),
        _silver_event(
            event_id="4",
            event_type="PushEvent",
            actor_id=13,
            repo_name="apache/airflow",
            activity_date=date(2025, 1, 3),
            commit_count=50,
        ),
    ]

    with open_local_catalog(
        catalog_name="test",
        catalog_path=tmp_path / "catalog.db",
        warehouse_path=tmp_path / "warehouse",
    ) as catalog:
        silver_table = ensure_silver_events_table(catalog)
        gold_table = ensure_gold_daily_activity_table(catalog)
        append_silver_events(silver_table, events)

        written_rows = load_silver_date_to_gold(
            silver_table=silver_table,
            gold_table=gold_table,
            activity_date=target_date,
            technologies=[airflow],
        )

        rows = catalog.load_table("gold.daily_technology_activity").scan().to_arrow().to_pylist()

    assert written_rows == 1
    assert len(rows) == 1
    assert rows[0]["technology_id"] == "apache-airflow"
    assert rows[0]["activity_date"] == target_date
    assert rows[0]["event_count"] == 2
    assert rows[0]["unique_actor_count"] == 2
    assert rows[0]["commit_count"] == 3
    assert rows[0]["star_count"] == 1


def test_load_silver_date_to_gold_replaces_previous_result(
    tmp_path: Path,
) -> None:
    target_date = date(2025, 1, 2)
    airflow = Technology(
        technology_id="apache-airflow",
        name="Apache Airflow",
        category="orchestration",
        repositories=("apache/airflow",),
    )

    first_event = _silver_event(
        event_id="1",
        event_type="PushEvent",
        actor_id=10,
        repo_name="apache/airflow",
        activity_date=target_date,
        commit_count=2,
    )
    second_event = _silver_event(
        event_id="2",
        event_type="WatchEvent",
        actor_id=11,
        repo_name="apache/airflow",
        activity_date=target_date,
        action="started",
    )

    with open_local_catalog(
        catalog_name="test",
        catalog_path=tmp_path / "catalog.db",
        warehouse_path=tmp_path / "warehouse",
    ) as catalog:
        silver_table = ensure_silver_events_table(catalog)
        gold_table = ensure_gold_daily_activity_table(catalog)

        append_silver_events(
            silver_table,
            [first_event],
        )
        first_written_rows = load_silver_date_to_gold(
            silver_table=silver_table,
            gold_table=gold_table,
            activity_date=target_date,
            technologies=[airflow],
        )

        append_silver_events(
            silver_table,
            [second_event],
        )
        second_written_rows = load_silver_date_to_gold(
            silver_table=silver_table,
            gold_table=gold_table,
            activity_date=target_date,
            technologies=[airflow],
        )

        rows = catalog.load_table("gold.daily_technology_activity").scan().to_arrow().to_pylist()

    assert first_written_rows == 1
    assert second_written_rows == 1
    assert len(rows) == 1
    assert rows[0]["event_count"] == 2
    assert rows[0]["unique_actor_count"] == 2
    assert rows[0]["commit_count"] == 2
    assert rows[0]["star_count"] == 1
