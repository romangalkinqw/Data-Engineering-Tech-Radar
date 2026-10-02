from dataclasses import replace
from datetime import UTC, date, datetime
from unittest.mock import MagicMock

from de_tech_radar.gold.daily_activity import (
    DailyTechnologyActivity,
)
from de_tech_radar.orchestration.assets import (
    daily_technology_activity,
)
from de_tech_radar.orchestration.checks import (
    complete_archive_hours,
    evaluate_complete_archive_hours,
    evaluate_unique_gold_keys,
    evaluate_valid_gold_metrics,
    find_missing_archive_hours,
    load_silver_archive_hours,
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


def test_find_missing_archive_hours() -> None:
    activity_date = date(2025, 1, 2)
    present_hours = [
        datetime(
            2025,
            1,
            2,
            hour,
            tzinfo=UTC,
        )
        for hour in range(24)
        if hour not in {3, 17}
    ]

    missing_hours = find_missing_archive_hours(
        activity_date=activity_date,
        archive_hours=present_hours,
    )

    assert missing_hours == (
        datetime(2025, 1, 2, 3, tzinfo=UTC),
        datetime(2025, 1, 2, 17, tzinfo=UTC),
    )


def test_complete_archive_hours_rejects_missing_hours() -> None:
    activity_date = date(2025, 1, 2)
    present_hours = [
        datetime(
            2025,
            1,
            2,
            hour,
            tzinfo=UTC,
        )
        for hour in range(24)
        if hour not in {3, 17}
    ]

    result = evaluate_complete_archive_hours(
        activity_date=activity_date,
        archive_hours=present_hours,
    )

    assert not result.passed
    assert result.metadata["expected_hour_count"].value == 24
    assert result.metadata["present_hour_count"].value == 22
    assert result.metadata["missing_hour_count"].value == 2
    assert result.metadata["missing_hours"].value == [
        "2025-01-02T03:00:00+00:00",
        "2025-01-02T17:00:00+00:00",
    ]


def test_complete_archive_hours_accepts_all_hours() -> None:
    activity_date = date(2025, 1, 2)
    present_hours = [
        datetime(
            2025,
            1,
            2,
            hour,
            tzinfo=UTC,
        )
        for hour in range(24)
    ]

    result = evaluate_complete_archive_hours(
        activity_date=activity_date,
        archive_hours=present_hours,
    )

    assert result.passed
    assert result.metadata["present_hour_count"].value == 24
    assert result.metadata["missing_hour_count"].value == 0
    assert result.metadata["missing_hours"].value == []


def test_load_silver_archive_hours_returns_sorted_unique_hours() -> None:
    first_hour = datetime(2025, 1, 2, 3, tzinfo=UTC)
    second_hour = datetime(2025, 1, 2, 17, tzinfo=UTC)
    table = MagicMock()

    (table.scan.return_value.to_arrow.return_value.to_pylist).return_value = [
        {"archive_hour": second_hour},
        {"archive_hour": first_hour},
        {"archive_hour": first_hour},
    ]

    result = load_silver_archive_hours(
        table=table,
        activity_date=date(2025, 1, 2),
    )

    assert result == (
        first_hour,
        second_hour,
    )
    table.refresh.assert_called_once_with()


def test_complete_archive_hours_targets_daily_asset() -> None:
    check_key = complete_archive_hours.check_key

    assert check_key.asset_key == daily_technology_activity.key
    assert check_key.name == "complete_archive_hours"
