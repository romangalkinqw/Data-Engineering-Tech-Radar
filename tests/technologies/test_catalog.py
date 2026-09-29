from pathlib import Path

import pytest

from de_tech_radar.technologies.catalog import (
    Technology,
    build_repository_index,
    find_technology,
    load_technology_catalog,
)


def test_load_technology_catalog(
    tmp_path: Path,
) -> None:
    catalog_path = tmp_path / "technologies.toml"
    catalog_path.write_text(
        """
[[technologies]]
id = "apache-airflow"
name = "Apache Airflow"
category = "orchestration"
repositories = ["apache/airflow"]
""".strip(),
        encoding="utf-8",
    )

    technologies = load_technology_catalog(catalog_path)

    assert technologies == [
        Technology(
            technology_id="apache-airflow",
            name="Apache Airflow",
            category="orchestration",
            repositories=("apache/airflow",),
        )
    ]


def test_load_technology_catalog_rejects_duplicate_repository(
    tmp_path: Path,
) -> None:
    catalog_path = tmp_path / "technologies.toml"
    catalog_path.write_text(
        """
[[technologies]]
id = "apache-airflow"
name = "Apache Airflow"
category = "orchestration"
repositories = ["Apache/Airflow"]

[[technologies]]
id = "airflow-alias"
name = "Airflow Alias"
category = "orchestration"
repositories = ["apache/airflow"]
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(
        ValueError,
        match=("repository 'apache/airflow' is assigned to multiple technologies"),
    ):
        load_technology_catalog(catalog_path)


def test_load_technology_catalog_rejects_duplicate_id(
    tmp_path: Path,
) -> None:
    catalog_path = tmp_path / "technologies.toml"
    catalog_path.write_text(
        """
[[technologies]]
id = "apache-airflow"
name = "Apache Airflow"
category = "orchestration"
repositories = ["apache/airflow"]

[[technologies]]
id = "Apache-Airflow"
name = "Airflow Duplicate"
category = "orchestration"
repositories = ["example/airflow"]
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(
        ValueError,
        match="technology id 'apache-airflow' is duplicated",
    ):
        load_technology_catalog(catalog_path)


def test_load_technology_catalog_rejects_invalid_repository(
    tmp_path: Path,
) -> None:
    catalog_path = tmp_path / "technologies.toml"
    catalog_path.write_text(
        """
[[technologies]]
id = "apache-airflow"
name = "Apache Airflow"
category = "orchestration"
repositories = ["apache-airflow"]
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(
        ValueError,
        match=("repository 'apache-airflow' must have 'owner/name' format"),
    ):
        load_technology_catalog(catalog_path)


def test_project_technology_catalog_is_valid() -> None:
    project_root = Path(__file__).resolve().parents[2]
    catalog_path = project_root / "config" / "technologies.toml"

    technologies = load_technology_catalog(catalog_path)

    assert len(technologies) >= 10
    assert all(technology.repositories for technology in technologies)


def test_build_repository_index_maps_all_repositories() -> None:
    technology = Technology(
        technology_id="apache-airflow",
        name="Apache Airflow",
        category="orchestration",
        repositories=(
            "apache/airflow",
            "apache/airflow-client-python",
        ),
    )

    repository_index = build_repository_index([technology])

    assert repository_index == {
        "apache/airflow": technology,
        "apache/airflow-client-python": technology,
    }


def test_find_technology_normalizes_repository_name() -> None:
    technology = Technology(
        technology_id="apache-airflow",
        name="Apache Airflow",
        category="orchestration",
        repositories=("apache/airflow",),
    )
    repository_index = build_repository_index([technology])

    found = find_technology(
        repository_index,
        "Apache/Airflow",
    )
    missing = find_technology(
        repository_index,
        "example/unknown",
    )

    assert found == technology
    assert missing is None
