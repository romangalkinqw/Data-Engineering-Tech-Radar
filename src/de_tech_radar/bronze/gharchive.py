import gzip
import json
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import cast


@dataclass(frozen=True, slots=True)
class BronzeEvent:
    event_id: str
    event_type: str
    created_at: datetime
    is_public: bool
    actor_id: int
    actor_login: str
    repo_id: int
    repo_name: str
    org_id: int | None
    org_login: str | None
    payload_json: str
    archive_hour: datetime
    source_file: str
    source_line_number: int


class EventParseError(ValueError):
    """Raised when a GH Archive event cannot become a Bronze record."""


def _parse_event_line(
    line: str,
    *,
    source_file: str,
    source_line_number: int,
    archive_hour: datetime,
) -> BronzeEvent:
    """Parse one GH Archive JSON line into a Bronze record."""
    decoded: object = json.loads(line)
    if not isinstance(decoded, dict):
        raise ValueError("event must be a JSON object")

    event = cast(dict[str, object], decoded)
    actor = _require_object(event, "actor")
    repo = _require_object(event, "repo")
    payload = _require_object(event, "payload")
    org = _optional_object(event, "org")

    return BronzeEvent(
        event_id=_require_field(event, "id", str),
        event_type=_require_field(event, "type", str),
        created_at=_to_utc(datetime.fromisoformat(_require_field(event, "created_at", str))),
        is_public=_require_field(event, "public", bool),
        actor_id=_require_field(
            actor,
            "id",
            int,
            field_path="actor.id",
        ),
        actor_login=_require_field(
            actor,
            "login",
            str,
            field_path="actor.login",
        ),
        repo_id=_require_field(
            repo,
            "id",
            int,
            field_path="repo.id",
        ),
        repo_name=_require_field(
            repo,
            "name",
            str,
            field_path="repo.name",
        ),
        org_id=(_require_field(org, "id", int, field_path="org.id") if org is not None else None),
        org_login=(
            _require_field(org, "login", str, field_path="org.login") if org is not None else None
        ),
        payload_json=json.dumps(
            payload,
            ensure_ascii=False,
            separators=(",", ":"),
        ),
        archive_hour=_to_utc(archive_hour),
        source_file=source_file,
        source_line_number=source_line_number,
    )


def parse_event_line(
    line: str,
    *,
    source_file: str,
    source_line_number: int,
    archive_hour: datetime,
) -> BronzeEvent:
    """Parse an event and attach source coordinates to data errors."""
    try:
        return _parse_event_line(
            line,
            source_file=source_file,
            source_line_number=source_line_number,
            archive_hour=archive_hour,
        )
    except ValueError as error:
        raise EventParseError(f"{source_file}:{source_line_number}: {error}") from error


def iter_archive_events(
    archive_path: Path,
    *,
    archive_hour: datetime,
) -> Iterator[BronzeEvent]:
    """Yield Bronze records from a compressed GH Archive file."""
    with gzip.open(
        archive_path,
        "rt",
        encoding="utf-8",
    ) as stream:
        for line_number, line in enumerate(stream, start=1):
            yield parse_event_line(
                line,
                source_file=archive_path.as_posix(),
                source_line_number=line_number,
                archive_hour=archive_hour,
            )


def _require_field[T](
    data: dict[str, object],
    field: str,
    expected_type: type[T],
    *,
    field_path: str | None = None,
) -> T:
    path = field_path or field

    if field not in data:
        raise ValueError(f"field '{path}' is required")

    value = data[field]

    if not isinstance(value, expected_type):
        raise ValueError(
            f"field '{path}' must be {expected_type.__name__}, got {type(value).__name__}"
        )

    # bool является подклассом int, но для JSON-контракта это разные типы.
    if type(value) is not expected_type:
        raise ValueError(
            f"field '{path}' must be {expected_type.__name__}, got {type(value).__name__}"
        )

    return value


def _require_object(
    data: dict[str, object],
    field: str,
) -> dict[str, object]:
    if field not in data:
        raise ValueError(f"field '{field}' is required")

    value = data[field]

    if not isinstance(value, dict):
        raise ValueError(f"field '{field}' must be dict")

    return cast(dict[str, object], value)


def _optional_object(
    data: dict[str, object],
    field: str,
) -> dict[str, object] | None:
    value = data.get(field)

    if value is None:
        return None

    if not isinstance(value, dict):
        raise ValueError(f"field '{field}' must be dict")

    return cast(dict[str, object], value)


def _to_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("datetime must be timezone-aware")

    return value.astimezone(UTC)
