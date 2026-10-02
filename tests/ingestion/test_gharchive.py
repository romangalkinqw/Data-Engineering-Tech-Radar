import gzip
import logging
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path

import httpx
import pytest

from de_tech_radar.ingestion.gharchive import (
    ArchiveDownloadResult,
    build_archive_path,
    build_archive_url,
    download_archive,
    download_archive_with_metrics,
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
    caplog: pytest.LogCaptureFixture,
) -> None:
    archive_hour = datetime(2025, 1, 2, 3, tzinfo=UTC)
    archive_bytes = gzip.compress(b'{"type":"PushEvent"}\n')
    caplog.set_level(
        logging.WARNING,
        logger="de_tech_radar.ingestion.gharchive",
    )

    def handle_request(request: httpx.Request) -> httpx.Response:
        assert request.method == "GET"
        assert str(request.url) == ("https://data.gharchive.org/2025-01-02-3.json.gz")
        return httpx.Response(
            200,
            stream=httpx.ByteStream(archive_bytes),
        )

    transport = httpx.MockTransport(handle_request)

    with httpx.Client(transport=transport) as client:
        result = download_archive_with_metrics(
            archive_hour=archive_hour,
            raw_root=tmp_path,
            client=client,
        )

    expected_path = build_archive_path(tmp_path, archive_hour)

    assert result == ArchiveDownloadResult(
        path=expected_path,
        downloaded=True,
        retry_count=0,
    )
    assert expected_path.read_bytes() == archive_bytes
    assert caplog.messages == []


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
                retry_backoff_seconds=0,
            )

    assert not archive_path.exists()
    assert not partial_path.exists(), "partial file must be removed"


def test_download_archive_retries_connect_timeout(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    archive_hour = datetime(2025, 1, 2, 3, tzinfo=UTC)
    archive_bytes = gzip.compress(b'{"type":"PushEvent"}\n')
    attempt_count = 0

    def handle_request(
        request: httpx.Request,
    ) -> httpx.Response:
        nonlocal attempt_count
        attempt_count += 1

        if attempt_count < 3:
            raise httpx.ConnectTimeout(
                "connection timed out",
                request=request,
            )

        return httpx.Response(
            200,
            stream=httpx.ByteStream(archive_bytes),
        )

    caplog.set_level(
        logging.WARNING,
        logger="de_tech_radar.ingestion.gharchive",
    )

    transport = httpx.MockTransport(handle_request)

    with httpx.Client(transport=transport) as client:
        result = download_archive_with_metrics(
            archive_hour=archive_hour,
            raw_root=tmp_path,
            client=client,
            max_attempts=3,
            retry_backoff_seconds=0,
        )

    assert attempt_count == 3
    assert result.downloaded is True
    assert result.retry_count == 2
    assert result.path.read_bytes() == archive_bytes
    assert caplog.messages == [
        (
            "Retrying GH Archive download "
            "url=https://data.gharchive.org/"
            "2025-01-02-3.json.gz "
            "attempt=1/3 delay_seconds=0 "
            "error=ConnectTimeout"
        ),
        (
            "Retrying GH Archive download "
            "url=https://data.gharchive.org/"
            "2025-01-02-3.json.gz "
            "attempt=2/3 delay_seconds=0 "
            "error=ConnectTimeout"
        ),
    ]


def test_download_archive_retries_server_error(
    tmp_path: Path,
) -> None:
    archive_hour = datetime(2025, 1, 2, 3, tzinfo=UTC)
    archive_bytes = gzip.compress(b'{"type":"PushEvent"}\n')
    attempt_count = 0

    def handle_request(
        request: httpx.Request,
    ) -> httpx.Response:
        nonlocal attempt_count
        attempt_count += 1

        if attempt_count == 1:
            return httpx.Response(
                503,
                request=request,
            )

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
            max_attempts=2,
            retry_backoff_seconds=0,
        )

    assert attempt_count == 2
    assert downloaded_path.read_bytes() == archive_bytes


def test_download_archive_does_not_retry_not_found(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    archive_hour = datetime(2025, 1, 2, 3, tzinfo=UTC)
    attempt_count = 0

    def handle_request(
        request: httpx.Request,
    ) -> httpx.Response:
        nonlocal attempt_count
        attempt_count += 1

        return httpx.Response(
            404,
            request=request,
        )

    transport = httpx.MockTransport(handle_request)

    caplog.set_level(
        logging.ERROR,
        logger="de_tech_radar.ingestion.gharchive",
    )

    with httpx.Client(transport=transport) as client:
        with pytest.raises(
            httpx.HTTPStatusError,
            match="404",
        ):
            download_archive(
                archive_hour=archive_hour,
                raw_root=tmp_path,
                client=client,
                max_attempts=3,
                retry_backoff_seconds=0,
            )

    assert attempt_count == 1
    assert caplog.messages == [
        (
            "GH Archive download failed "
            "url=https://data.gharchive.org/"
            "2025-01-02-3.json.gz "
            "attempts=1 error=HTTPStatusError"
        )
    ]


def test_download_archive_stops_after_max_attempts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    archive_hour = datetime(2025, 1, 2, 3, tzinfo=UTC)
    archive_path = build_archive_path(
        tmp_path,
        archive_hour,
    )
    partial_path = archive_path.with_name(f"{archive_path.name}.part")
    attempt_count = 0
    delays: list[float] = []

    def handle_request(
        request: httpx.Request,
    ) -> httpx.Response:
        nonlocal attempt_count
        attempt_count += 1

        raise httpx.ConnectTimeout(
            "connection timed out",
            request=request,
        )

    monkeypatch.setattr(
        "de_tech_radar.ingestion.gharchive.time.sleep",
        delays.append,
    )
    transport = httpx.MockTransport(handle_request)

    caplog.set_level(
        logging.ERROR,
        logger="de_tech_radar.ingestion.gharchive",
    )

    with httpx.Client(transport=transport) as client:
        with pytest.raises(
            httpx.ConnectTimeout,
            match="connection timed out",
        ):
            download_archive(
                archive_hour=archive_hour,
                raw_root=tmp_path,
                client=client,
                max_attempts=3,
                retry_backoff_seconds=0.25,
            )

    assert attempt_count == 3
    assert delays == [0.25, 0.5]
    assert not archive_path.exists()
    assert not partial_path.exists()
    assert caplog.messages == [
        (
            "GH Archive download failed "
            "url=https://data.gharchive.org/"
            "2025-01-02-3.json.gz "
            "attempts=3 error=ConnectTimeout"
        )
    ]


def test_download_archive_with_metrics_reports_new_file(
    tmp_path: Path,
) -> None:
    archive_hour = datetime(2025, 1, 2, 3, tzinfo=UTC)
    archive_bytes = gzip.compress(b'{"type":"PushEvent"}\n')

    def handle_request(
        _: httpx.Request,
    ) -> httpx.Response:
        return httpx.Response(
            200,
            stream=httpx.ByteStream(archive_bytes),
        )

    transport = httpx.MockTransport(handle_request)

    with httpx.Client(transport=transport) as client:
        result = download_archive_with_metrics(
            archive_hour=archive_hour,
            raw_root=tmp_path,
            client=client,
        )

    expected_path = build_archive_path(
        tmp_path,
        archive_hour,
    )

    assert result == ArchiveDownloadResult(
        path=expected_path,
        downloaded=True,
        retry_count=0,
    )
    assert expected_path.read_bytes() == archive_bytes


def test_download_archive_with_metrics_reports_reused_file(
    tmp_path: Path,
) -> None:
    archive_hour = datetime(2025, 1, 2, 3, tzinfo=UTC)
    archive_path = build_archive_path(
        tmp_path,
        archive_hour,
    )
    archive_path.parent.mkdir(parents=True)
    archive_path.write_bytes(b"existing archive")

    def reject_request(
        _: httpx.Request,
    ) -> httpx.Response:
        raise AssertionError("HTTP request must not be made")

    transport = httpx.MockTransport(reject_request)

    with httpx.Client(transport=transport) as client:
        result = download_archive_with_metrics(
            archive_hour=archive_hour,
            raw_root=tmp_path,
            client=client,
        )

    assert result == ArchiveDownloadResult(
        path=archive_path,
        downloaded=False,
        retry_count=0,
    )
