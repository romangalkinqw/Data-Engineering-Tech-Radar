import dagster as dg

from de_tech_radar.orchestration.definitions import defs


def test_dagster_definitions_are_loadable() -> None:
    dg.Definitions.validate_loadable(defs)


def test_daily_schedule_runs_at_two_utc() -> None:
    schedule = defs.resolve_schedule_def("daily_technology_activity_schedule")

    assert schedule.job_name == ("daily_technology_activity_job")
    assert schedule.cron_schedule == "0 2 * * *"
    assert schedule.execution_timezone == "UTC"
