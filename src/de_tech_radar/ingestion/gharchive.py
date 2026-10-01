import time
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
    *,
    max_attempts: int = 3,
    retry_backoff_seconds: float = 1.0,
) -> Path:
    """Download one GH Archive hour into the raw zone."""
    if max_attempts < 1:
        raise ValueError("max_attempts must be at least 1")

    if retry_backoff_seconds < 0:
        raise ValueError("retry_backoff_seconds must be non-negative")

    archive_path = build_archive_path(
        raw_root,
        archive_hour,
    )

    if archive_path.exists():
        return archive_path

    archive_url = build_archive_url(archive_hour)
    temporary_path = archive_path.with_name(f"{archive_path.name}.part")

    archive_path.parent.mkdir(parents=True, exist_ok=True)

    for attempt_number in range(1, max_attempts + 1):
        try:
            with client.stream(
                "GET",
                archive_url,
            ) as response:
                response.raise_for_status()

                with temporary_path.open("wb") as output_file:
                    for chunk in response.iter_raw():
                        output_file.write(chunk)

            temporary_path.replace(archive_path)
            return archive_path
        except (
            httpx.TimeoutException,
            httpx.NetworkError,
            httpx.HTTPStatusError,
        ) as error:
            if isinstance(error, httpx.HTTPStatusError):
                status_code = error.response.status_code
                is_retryable_status = status_code == 429 or 500 <= status_code < 600

                if not is_retryable_status:
                    raise

            if attempt_number == max_attempts:
                raise

            delay_seconds = retry_backoff_seconds * (2 ** (attempt_number - 1))
            time.sleep(delay_seconds)

        finally:
            temporary_path.unlink(missing_ok=True)

    raise AssertionError("retry loop finished unexpectedly")
