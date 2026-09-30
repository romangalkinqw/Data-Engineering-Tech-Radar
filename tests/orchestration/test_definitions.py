import dagster as dg

from de_tech_radar.orchestration.checks import (
    unique_gold_keys,
    valid_gold_metrics,
)
from de_tech_radar.orchestration.definitions import defs


def test_dagster_definitions_are_loadable() -> None:
    dg.Definitions.validate_loadable(defs)


def test_daily_schedule_runs_at_two_utc() -> None:
    schedule = defs.resolve_schedule_def("daily_technology_activity_schedule")

    assert schedule.job_name == ("daily_technology_activity_job")
    assert schedule.cron_schedule == "0 2 * * *"
    assert schedule.execution_timezone == "UTC"


def test_definitions_register_unique_gold_keys() -> None:
    asset_graph = defs.resolve_asset_graph()

    assert unique_gold_keys.check_key in asset_graph.asset_check_keys


def test_definitions_register_valid_gold_metrics() -> None:
    asset_graph = defs.resolve_asset_graph()

    assert valid_gold_metrics.check_key in asset_graph.asset_check_keys


def test_daily_job_includes_postgres_asset() -> None:
    asset_graph = defs.resolve_asset_graph()
    postgres_key = dg.AssetKey("postgres_daily_technology_activity")

    assert asset_graph.has(postgres_key)

    job = defs.resolve_job_def("daily_technology_activity_job")

    assert job.asset_layer.executable_asset_keys == {
        dg.AssetKey("daily_technology_activity"),
        postgres_key,
    }
