from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from pyiceberg.catalog.sql import SqlCatalog


@contextmanager
def open_local_catalog(
    *,
    catalog_name: str,
    catalog_path: Path,
    warehouse_path: Path,
) -> Iterator[SqlCatalog]:
    """Open a local SQLite-backed Iceberg catalog."""
    resolved_catalog_path = catalog_path.resolve()
    resolved_warehouse_path = warehouse_path.resolve()

    resolved_catalog_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )
    resolved_warehouse_path.mkdir(
        parents=True,
        exist_ok=True,
    )

    catalog = SqlCatalog(
        catalog_name,
        uri=f"sqlite:///{resolved_catalog_path.as_posix()}",
        warehouse=resolved_warehouse_path.as_posix(),
    )

    try:
        yield catalog
    finally:
        catalog.engine.dispose()
