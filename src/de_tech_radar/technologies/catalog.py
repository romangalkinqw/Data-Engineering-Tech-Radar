import tomllib
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class Technology:
    technology_id: str
    name: str
    category: str
    repositories: tuple[str, ...]


def build_repository_index(
    technologies: Iterable[Technology],
) -> dict[str, Technology]:
    """Build a normalized repository-to-technology index."""
    return {
        repository.strip().casefold(): technology
        for technology in technologies
        for repository in technology.repositories
    }


def find_technology(
    repository_index: Mapping[str, Technology],
    repository_name: str,
) -> Technology | None:
    """Find a technology by a GitHub repository name."""
    normalized_repository = repository_name.strip().casefold()

    return repository_index.get(normalized_repository)


def load_technology_catalog(
    catalog_path: Path,
) -> list[Technology]:
    """Load and validate technology definitions from TOML."""
    with catalog_path.open("rb") as stream:
        document = tomllib.load(stream)

    technologies: list[Technology] = []
    technology_ids: set[str] = set()
    repository_owners: dict[str, str] = {}

    for item in document["technologies"]:
        technology_id = item["id"].strip().casefold()

        if technology_id in technology_ids:
            raise ValueError(f"technology id '{technology_id}' is duplicated")

        technology_ids.add(technology_id)

        repositories = tuple(repository.strip().casefold() for repository in item["repositories"])

        for repository in repositories:
            repository_parts = repository.split("/")

            if len(repository_parts) != 2 or not repository_parts[0] or not repository_parts[1]:
                raise ValueError(f"repository '{repository}' must have 'owner/name' format")

            if repository in repository_owners:
                raise ValueError(f"repository '{repository}' is assigned to multiple technologies")

            repository_owners[repository] = technology_id

            repository_owners[repository] = technology_id

        technologies.append(
            Technology(
                technology_id=technology_id,
                name=item["name"],
                category=item["category"],
                repositories=repositories,
            )
        )

    return technologies
