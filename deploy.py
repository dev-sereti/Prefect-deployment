from datetime import timedelta

from prefect.client.schemas.schedules import IntervalSchedule
from prefect.runner.storage import GitRepository

from pipeline import teen_mental_health_pipeline


if __name__ == "__main__":

    teen_mental_health_pipeline.deploy(
        name="teen-mental-health-etl",

        # Prefect managed pool
        work_pool_name="default-work-pool",

        # GitHub repo containing your flow code
        storage=GitRepository(
            url="https://github.com/dev-sereti/Prefect-deployment.git",
            branch="main",
        ),

        # Run every 60 minutes
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
    print("View runs at: https://app.prefect.io")