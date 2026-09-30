from dataclasses import replace
from datetime import date

from de_tech_radar.gold.daily_activity import (
    DailyTechnologyActivity,
)
from de_tech_radar.orchestration.assets import (
    daily_technology_activity,
)
from de_tech_radar.orchestration.checks import (
    evaluate_unique_gold_keys,
    evaluate_valid_gold_metrics,
    unique_gold_keys,
    valid_gold_metrics,
)


def _valid_activity() -> DailyTechnologyActivity:
    return DailyTechnologyActivity(
        technology_id="apache-spark",
        activity_date=date(2025, 1, 2),
        event_count=1,
        unique_actor_count=1,
        push_count=0,
        commit_count=0,
        pull_request_count=0,
        merged_pull_request_count=0,
        issue_count=0,
        star_count=0,
        fork_count=0,
        release_count=0,
    )


def test_unique_gold_keys_rejects_duplicate_key() -> None:
    activity_date = date(2025, 1, 2)

    result = evaluate_unique_gold_keys(
        [
            ("apache-spark", activity_date),
            ("apache-kafka", activity_date),
            ("apache-spark", activity_date),
        ]
    )

    assert not result.passed
    assert result.metadata["duplicate_key_count"].value == 1
    assert result.metadata["row_count"].value == 3


def test_unique_gold_keys_targets_daily_asset() -> None:
    check_key = unique_gold_keys.check_key

    assert check_key.asset_key == daily_technology_activity.key
    assert check_key.name == "unique_gold_keys"


def test_gold_metrics_reject_negative_values() -> None:
    activity = replace(
        _valid_activity(),
        star_count=-1,
    )

    result = evaluate_valid_gold_metrics([activity])

    assert not result.passed
    assert result.metadata["invalid_row_count"].value == 1


def test_valid_gold_metrics_targets_daily_asset() -> None:
    check_key = valid_gold_metrics.check_key

    assert check_key.asset_key == daily_technology_activity.key
    assert check_key.name == "valid_gold_metrics"


def test_gold_metrics_reject_inconsistent_pr_counts() -> None:
    activity = replace(
        _valid_activity(),
        pull_request_count=0,
        merged_pull_request_count=1,
    )

    result = evaluate_valid_gold_metrics([activity])

    assert not result.passed
    assert result.metadata["invalid_row_count"].value == 1
