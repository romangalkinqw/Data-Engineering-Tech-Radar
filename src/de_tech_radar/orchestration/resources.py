from pathlib import Path
from typing import Any

import dagster as dg
import psycopg

_DEFAULT_PROJECT_ROOT = Path(__file__).resolve().parents[3]


class RadarPaths(dg.ConfigurableResource["RadarPaths"]):
    """Filesystem paths used by the data pipeline."""

    project_root: str = str(_DEFAULT_PROJECT_ROOT)
    raw_root: str = "data/raw"
    catalog_path: str = "data/lakehouse/catalog.db"
    warehouse_path: str = "data/lakehouse/warehouse"
    technology_catalog_path: str = "config/technologies.toml"

    def resolve(self, configured_path: str) -> Path:
        """Resolve a configured path from the project root."""

        path = Path(configured_path)

        if path.is_absolute():
            return path

        return Path(self.project_root) / path


class PostgresServing(dg.ConfigurableResource["PostgresServing"]):
    """PostgreSQL serving-layer connection settings."""

    dsn: str
    connect_timeout: int = 5

    def connect(
        self,
    ) -> psycopg.Connection[tuple[Any, ...]]:
        """Open a PostgreSQL connection."""

        return psycopg.connect(
            self.dsn,
            connect_timeout=self.connect_timeout,
        )
