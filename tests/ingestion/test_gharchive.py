import gzip
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path

import httpx
import pytest

from de_tech_radar.ingestion.gharchive import (
    build_archive_path,
    build_archive_url,
    download_archive,
)


def test_build_archive_url_for_utc_hour() -> None:
    archive_hour = datetime(2026, 9, 1, 3, tzinfo=UTC)

    assert build_archive_url(archive_hour) == "https://data.gharchive.org/2026-09-01-3.json.gz"


def test_build_archive_url_converts_offset_to_utc() -> None:
    moscow_timezone = timezone(timedelta(hours=3))
    archive_hour = datetime(2026, 9, 1, 6, tzinfo=moscow_timezone)

    assert build_archive_url(archive_hour) == "https://data.gharchive.org/2026-09-01-3.json.gz"


def test_build_archive_url_rejects_naive_datetime() -> None:
    archive_hour = datetime(2026, 9, 1, 3)

    with pytest.raises(ValueError, match="timezone-aware"):
        build_archive_url(archive_hour)


def test_build_archive_url_rejects_partial_hour() -> None:
    archive_hour = datetime(2026, 9, 1, 3, 30, tzinfo=UTC)

    with pytest.raises(ValueError, match="full UTC hour"):
        build_archive_url(archive_hour)


def test_build_archive_path_uses_hive_partitions(tmp_path: Path) -> None:
    archive_hour = datetime(2026, 9, 1, 3, tzinfo=UTC)

    expected_path = (
        tmp_path
        / "gharchive"
        / "archive_date=2026-09-01"
        / "archive_hour=03"
        / "2026-09-01-3.json.gz"
    )

    assert build_archive_path(tmp_path, archive_hour) == expected_path


def test_download_archive_writes_raw_response(
    tmp_path: Path,
) -> None:
    archive_hour = datetime(2025, 1, 2, 3, tzinfo=UTC)
    archive_bytes = gzip.compress(b'{"type":"PushEvent"}\n')

    def handle_request(request: httpx.Request) -> httpx.Response:
        assert request.method == "GET"
        assert str(request.url) == ("https://data.gharchive.org/2025-01-02-3.json.gz")
        return httpx.Response(
            200,
            stream=httpx.ByteStream(archive_bytes),
        )

    transport = httpx.MockTransport(handle_request)

    with httpx.Client(transport=transport) as client:
        downloaded_path = download_archive(
            archive_hour=archive_hour,
            raw_root=tmp_path,
            client=client,
        )

    expected_path = build_archive_path(tmp_path, archive_hour)

    assert downloaded_path == expected_path
    assert expected_path.read_bytes() == archive_bytes


def test_download_archive_does_not_overwrite_existing_file(
    tmp_path: Path,
) -> None:
    archive_hour = datetime(2025, 1, 2, 3, tzinfo=UTC)
    archive_path = build_archive_path(tmp_path, archive_hour)
    existing_bytes = gzip.compress(b'{"existing":true}\n')

    archive_path.parent.mkdir(parents=True)
    archive_path.write_bytes(existing_bytes)

    def handle_request(_: httpx.Request) -> httpx.Response:
        raise AssertionError("HTTP request must not be made")

    transport = httpx.MockTransport(handle_request)

    with httpx.Client(transport=transport) as client:
        downloaded_path = download_archive(
            archive_hour=archive_hour,
            raw_root=tmp_path,
            client=client,
        )

    assert downloaded_path == archive_path
    assert archive_path.read_bytes() == existing_bytes


def test_download_archive_removes_partial_file_after_stream_error(
    tmp_path: Path,
) -> None:
    archive_hour = datetime(2025, 1, 2, 3, tzinfo=UTC)
    archive_path = build_archive_path(tmp_path, archive_hour)
    partial_path = archive_path.with_name(f"{archive_path.name}.part")

    class FailingStream(httpx.SyncByteStream):
        def __iter__(self) -> Iterator[bytes]:
            yield b"partially downloaded data"
            raise httpx.ReadError("connection interrupted")

    def handle_request(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, stream=FailingStream())

    transport = httpx.MockTransport(handle_request)

    with httpx.Client(transport=transport) as client:
        with pytest.raises(httpx.ReadError, match="connection interrupted"):
            download_archive(
                archive_hour=archive_hour,
                raw_root=tmp_path,
                client=client,
            )

    assert not archive_path.exists()
    assert not partial_path.exists(), "partial file must be removed"
