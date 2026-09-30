import dagster as dg

from de_tech_radar.orchestration.assets import (
    daily_technology_activity,
)
from de_tech_radar.orchestration.checks import (
    unique_gold_keys,
    valid_gold_metrics,
)
from de_tech_radar.orchestration.resources import RadarPaths

daily_technology_activity_job = dg.define_asset_job(
    name="daily_technology_activity_job",
    selection=[daily_technology_activity],
)

daily_technology_activity_schedule = dg.build_schedule_from_partitioned_job(
    daily_technology_activity_job,
    name="daily_technology_activity_schedule",
    hour_of_day=2,
    minute_of_hour=0,
)

defs = dg.Definitions(
    assets=[daily_technology_activity],
    asset_checks=[
        unique_gold_keys,
        valid_gold_metrics,
    ],
    resources={
        "paths": RadarPaths(),
    },
    jobs=[daily_technology_activity_job],
    schedules=[daily_technology_activity_schedule],
)
