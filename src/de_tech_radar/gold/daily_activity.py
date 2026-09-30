from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from datetime import date

from de_tech_radar.silver.gharchive import SilverEvent
from de_tech_radar.technologies.catalog import (
    Technology,
    find_technology,
)


@dataclass(frozen=True, slots=True)
class DailyTechnologyActivity:
    technology_id: str
    activity_date: date
    event_count: int
    unique_actor_count: int
    push_count: int
    commit_count: int
    pull_request_count: int
    merged_pull_request_count: int
    issue_count: int
    star_count: int
    fork_count: int
    release_count: int


@dataclass(slots=True)
class _ActivityAccumulator:
    event_count: int = 0
    actor_ids: set[int] = field(default_factory=set)
    push_count: int = 0
    commit_count: int = 0
    pull_request_count: int = 0
    merged_pull_request_count: int = 0
    issue_count: int = 0
    star_count: int = 0
    fork_count: int = 0
    release_count: int = 0


def aggregate_daily_activity(
    events: Iterable[SilverEvent],
    repository_index: Mapping[str, Technology],
) -> list[DailyTechnologyActivity]:
    """Aggregate Silver events by technology and date."""
    accumulators: dict[
        tuple[str, date],
        _ActivityAccumulator,
    ] = {}

    for event in events:
        technology = find_technology(
            repository_index,
            event.repo_name,
        )

        if technology is None:
            continue

        key = (
            technology.technology_id,
            event.event_date,
        )
        accumulator = accumulators.setdefault(
            key,
            _ActivityAccumulator(),
        )

        accumulator.event_count += 1
        accumulator.actor_ids.add(event.actor_id)

        if event.event_type == "PushEvent":
            accumulator.push_count += 1
            accumulator.commit_count += event.commit_count or 0
        elif event.event_type == "PullRequestEvent":
            accumulator.pull_request_count += 1

            if event.is_merged is True:
                accumulator.merged_pull_request_count += 1
        elif event.event_type == "IssuesEvent":
            accumulator.issue_count += 1
        elif event.event_type == "WatchEvent" and event.action == "started":
            accumulator.star_count += 1
        elif event.event_type == "ForkEvent":
            accumulator.fork_count += 1
        elif event.event_type == "ReleaseEvent":
            accumulator.release_count += 1

    return [
        DailyTechnologyActivity(
            technology_id=technology_id,
            activity_date=activity_date,
            event_count=accumulator.event_count,
            unique_actor_count=len(accumulator.actor_ids),
            push_count=accumulator.push_count,
            commit_count=accumulator.commit_count,
            pull_request_count=(accumulator.pull_request_count),
            merged_pull_request_count=(accumulator.merged_pull_request_count),
            issue_count=accumulator.issue_count,
            star_count=accumulator.star_count,
            fork_count=accumulator.fork_count,
            release_count=accumulator.release_count,
        )
        for (
            technology_id,
            activity_date,
        ), accumulator in sorted(accumulators.items())
    ]
