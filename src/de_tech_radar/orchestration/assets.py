from datetime import date

import dagster as dg
import httpx

from de_tech_radar.lakehouse.catalog import open_local_catalog
from de_tech_radar.lakehouse.tables import (
    ensure_bronze_events_table,
    ensure_gold_daily_activity_table,
    ensure_silver_events_table,
)
from de_tech_radar.orchestration.resources import (
    PostgresServing,
    RadarPaths,
)
from de_tech_radar.pipelines.daily import (
    DailyPipelineResult,
    run_daily_pipeline,
)
from de_tech_radar.pipelines.gold_postgres import (
    publish_gold_date_to_postgres,
)
from de_tech_radar.technologies.catalog import (
    load_technology_catalog,
)

DAILY_PARTITIONS = dg.DailyPartitionsDefinition(
    start_date="2015-01-01",
)


def run_daily_partition(
    *,
    activity_date: date,
    paths: RadarPaths,
) -> DailyPipelineResult:
    """Run one daily partition using local infrastructure."""

    technologies = load_technology_catalog(paths.resolve(paths.technology_catalog_path))

    with open_local_catalog(
        catalog_name="local",
        catalog_path=paths.resolve(paths.catalog_path),
        warehouse_path=paths.resolve(paths.warehouse_path),
    ) as catalog:
        bronze_table = ensure_bronze_events_table(catalog)
        silver_table = ensure_silver_events_table(catalog)
        gold_table = ensure_gold_daily_activity_table(catalog)

        with httpx.Client(
            follow_redirects=True,
            timeout=60.0,
        ) as client:
            return run_daily_pipeline(
                activity_date=activity_date,
                raw_root=paths.resolve(paths.raw_root),
                bronze_table=bronze_table,
                silver_table=silver_table,
                gold_table=gold_table,
                technologies=technologies,
                client=client,
            )


def publish_daily_partition(
    *,
    activity_date: date,
    paths: RadarPaths,
    postgres: PostgresServing,
) -> int:
    """Publish one Gold partition into PostgreSQL."""

    with open_local_catalog(
        catalog_name="local",
        catalog_path=paths.resolve(paths.catalog_path),
        warehouse_path=paths.resolve(paths.warehouse_path),
    ) as catalog:
        gold_table = ensure_gold_daily_activity_table(catalog)

        with postgres.connect() as connection:
            return publish_gold_date_to_postgres(
                gold_table=gold_table,
                connection=connection,
                activity_date=activity_date,
            )


@dg.asset(
    partitions_def=DAILY_PARTITIONS,
    group_name="tech_radar",
)
def daily_technology_activity(
    context: dg.AssetExecutionContext,
    paths: RadarPaths,
) -> dg.MaterializeResult[None]:
    """Build one daily Gold technology activity partition."""

    result = run_daily_partition(
        activity_date=date.fromisoformat(context.partition_key),
        paths=paths,
    )

    return dg.MaterializeResult(
        metadata={
            "archive_count": result.archive_count,
            "bronze_rows": result.bronze_rows,
            "silver_rows": result.silver_rows,
            "gold_rows": result.gold_rows,
        }
    )


@dg.asset(
    partitions_def=DAILY_PARTITIONS,
    group_name="tech_radar",
    deps=[daily_technology_activity],
)
def postgres_daily_technology_activity(
    context: dg.AssetExecutionContext,
    paths: RadarPaths,
    postgres: PostgresServing,
) -> dg.MaterializeResult[None]:
    """Publish one Gold partition into PostgreSQL."""

    published_rows = publish_daily_partition(
        activity_date=date.fromisoformat(context.partition_key),
        paths=paths,
        postgres=postgres,
    )

    return dg.MaterializeResult(
        metadata={
            "published_rows": published_rows,
        }
    )
