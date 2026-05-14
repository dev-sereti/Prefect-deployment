from datetime import timedelta

from prefect.client.schemas.schedules import IntervalSchedule
from prefect.runner.storage import GitRepository

from pipeline import teen_mental_health_pipeline


if __name__ == "__main__":

    source = GitRepository(
        url="https://github.com/dev-sereti/Prefect-deployment.git",
        branch="main",
    )

    teen_mental_health_pipeline.from_source(
        source=source,
        entrypoint="pipeline.py:teen_mental_health_pipeline",
    ).deploy(
        name="teen-mental-health-etl",

        work_pool_name="default-work-pool",

        schedules=[
            IntervalSchedule(
                interval=timedelta(minutes=60)
            )
        ],

        tags=["etl", "snowflake", "mental-health"],

        description=(
            "Loads teen mental health data "
            "from SharePoint Excel into Snowflake every 60 minutes."
        ),
    )

    print("\nDeployment registered on Prefect Cloud!")