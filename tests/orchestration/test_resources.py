from pathlib import Path

from de_tech_radar.orchestration.resources import RadarPaths


def test_radar_paths_have_local_defaults() -> None:
    paths = RadarPaths()

    assert paths.raw_root == "data/raw"
    assert paths.catalog_path == "data/lakehouse/catalog.db"
    assert paths.warehouse_path == "data/lakehouse/warehouse"
    assert paths.technology_catalog_path == ("config/technologies.toml")


def test_radar_paths_resolve_from_project_root(
    tmp_path: Path,
) -> None:
    paths = RadarPaths(
        project_root=str(tmp_path),
    )

    resolved_path = paths.resolve(paths.technology_catalog_path)

    assert resolved_path == (tmp_path / "config" / "technologies.toml")
