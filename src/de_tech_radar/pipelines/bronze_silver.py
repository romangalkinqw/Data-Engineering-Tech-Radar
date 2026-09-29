from typing import Any, cast

from pyiceberg.expressions import EqualTo, Reference
from pyiceberg.expressions.literals import literal
from pyiceberg.table import Table

from de_tech_radar.bronze.gharchive import BronzeEvent
from de_tech_radar.lakehouse.writer import append_silver_events
from de_tech_radar.silver.gharchive import to_silver_event

SILVER_SOURCE_FILE_PROPERTY = "de-tech-radar.bronze-source-file"


def _is_source_file_committed(
    table: Table,
    source_file: str,
) -> bool:
    table.refresh()

    return any(
        snapshot.summary is not None
        and snapshot.summary.get(SILVER_SOURCE_FILE_PROPERTY) == source_file
        for snapshot in table.snapshots()
    )


def load_bronze_source_to_silver(
    *,
    bronze_table: Table,
    silver_table: Table,
    source_file: str,
) -> int:
    """Transform one Bronze source file into Silver."""
    if _is_source_file_committed(
        silver_table,
        source_file,
    ):
        return 0

    bronze_table.refresh()

    reader = bronze_table.scan(
        row_filter=EqualTo(
            term=Reference("source_file"),
            value=literal(source_file),
        )
    ).to_arrow_batch_reader()

    written_rows = 0

    try:
        with silver_table.transaction() as transaction:
            for record_batch in reader:
                records = cast(
                    list[dict[str, Any]],
                    record_batch.to_pylist(),
                )
                silver_events = (to_silver_event(BronzeEvent(**record)) for record in records)

                written_rows += append_silver_events(
                    transaction,
                    silver_events,
                    snapshot_properties={
                        SILVER_SOURCE_FILE_PROPERTY: (source_file),
                    },
                )
            if written_rows == 0:
                raise ValueError(f"Bronze source file not found: {source_file}")
    finally:
        reader.close()

    return written_rows
