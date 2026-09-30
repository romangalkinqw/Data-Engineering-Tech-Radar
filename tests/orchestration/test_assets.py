from datetime import date

import dagster as dg
import pytest

import de_tech_radar.orchestration.assets as assets_module
from de_tech_radar.orchestration.assets import (
    DAILY_PARTITIONS,
    daily_technology_activity,
)
from de_tech_radar.orchestration.resources import RadarPaths
from de_tech_radar.pipelines.daily import DailyPipelineResult


def test_daily_activity_asset_uses_daily_partitions() -> None:
    assert isinstance(
        DAILY_PARTITIONS,
        dg.DailyPartitionsDefinition,
    )
    assert daily_technology_activity.partitions_def is DAILY_PARTITIONS


def test_daily_activity_asset_runs_requested_partition(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed_date: date | None = None

    def fake_run_daily_partition(
        *,
        activity_date: date,
        paths: RadarPaths,
    ) -> DailyPipelineResult:
        nonlocal observed_date
        observed_date = activity_date

        return DailyPipelineResult(
            archive_count=24,
            bronze_rows=100,
            silver_rows=100,
            gold_rows=2,
        )

    monkeypatch.setattr(
        assets_module,
        "run_daily_partition",
        fake_run_daily_partition,
    )

    result = dg.materialize(
        [daily_technology_activity],
        resources={"paths": RadarPaths()},
        partition_key="2025-01-02",
    )

    assert result.success
    assert observed_date == date(2025, 1, 2)
