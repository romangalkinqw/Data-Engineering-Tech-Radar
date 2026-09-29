from datetime import datetime
from itertools import batched
from pathlib import Path

from pyiceberg.table import Table

from de_tech_radar.bronze.gharchive import iter_archive_events
from de_tech_radar.lakehouse.writer import append_bronze_events

DEFAULT_BATCH_SIZE = 10_000
SOURCE_FILE_PROPERTY = "de-tech-radar.source-file"


def _is_source_file_committed(
    table: Table,
    source_file: str,
) -> bool:
    table.refresh()

    return any(
        snapshot.summary is not None and snapshot.summary.get(SOURCE_FILE_PROPERTY) == source_file
        for snapshot in table.snapshots()
    )


def load_archive_to_bronze(
    *,
    archive_path: Path,
    archive_hour: datetime,
    table: Table,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> int:
    """Parse an archive and append its events in bounded batches."""
    source_file = archive_path.name

    if _is_source_file_committed(table, source_file):
        return 0

    events = iter_archive_events(
        archive_path,
        archive_hour=archive_hour,
    )
    written_rows = 0

    with table.transaction() as transaction:
        for event_batch in batched(
            events,
            batch_size,
            strict=False,
        ):
            written_rows += append_bronze_events(
                transaction,
                event_batch,
                snapshot_properties={
                    SOURCE_FILE_PROPERTY: source_file,
                },
            )

    return written_rows
