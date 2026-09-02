from datetime import UTC, datetime
from pathlib import Path

import httpx

GH_ARCHIVE_BASE_URL = "https://data.gharchive.org"


def build_archive_url(archive_hour: datetime) -> str:
    """Build the GH Archive URL for an exact hourly boundary."""
    utc_hour = _normalize_archive_hour(archive_hour)

    return f"{GH_ARCHIVE_BASE_URL}/{_archive_filename(utc_hour)}"


def build_archive_path(raw_root: Path, archive_hour: datetime) -> Path:
    """Build the local raw path for a GH Archive file."""
    utc_hour = _normalize_archive_hour(archive_hour)

    return (
        raw_root
        / "gharchive"
        / f"archive_date={utc_hour:%Y-%m-%d}"
        / f"archive_hour={utc_hour.hour:02d}"
        / _archive_filename(utc_hour)
    )


def _normalize_archive_hour(archive_hour: datetime) -> datetime:
    if archive_hour.tzinfo is None or archive_hour.utcoffset() is None:
        raise ValueError("archive_hour must be timezone-aware")

    utc_hour = archive_hour.astimezone(UTC)

    if (utc_hour.minute, utc_hour.second, utc_hour.microsecond) != (0, 0, 0):
        raise ValueError("archive_hour must be aligned to a full UTC hour")

    return utc_hour


def _archive_filename(utc_hour: datetime) -> str:
    return f"{utc_hour:%Y-%m-%d}-{utc_hour.hour}.json.gz"


def download_archive(
    archive_hour: datetime,
    raw_root: Path,
    client: httpx.Client,
) -> Path:
    """Download one GH Archive hour into the raw zone."""
    archive_path = build_archive_path(raw_root, archive_hour)

    if archive_path.exists():
        return archive_path

    archive_url = build_archive_url(archive_hour)
    temporary_path = archive_path.with_name(f"{archive_path.name}.part")

    archive_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        with client.stream("GET", archive_url) as response:
            response.raise_for_status()

            with temporary_path.open("wb") as output_file:
                for chunk in response.iter_raw():
                    output_file.write(chunk)

        temporary_path.replace(archive_path)
    finally:
        temporary_path.unlink(missing_ok=True)

    return archive_path
