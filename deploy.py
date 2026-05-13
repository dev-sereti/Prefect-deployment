from prefect.client.schemas.schedules import IntervalSchedule
from datetime import timedelta
from pipeline import teen_mental_health_pipeline   # import your flow


if __name__ == "__main__":
    teen_mental_health_pipeline.deploy(
        name="teen-mental-health-etl",

        #  Work pool that will run the flow 
        work_pool_name="default-work-pool",   # your existing managed pool

        #  Schedule: every 60 minutes 
        schedules=[
            IntervalSchedule(interval=timedelta(minutes=60))
        ],

        #  Where Prefect pulls your code from 
        # Option A — run from local files (good for development)
        # (no extra config needed; worker must have access to this path)

        # Option B — pull from a GitHub repo (recommended for production)
        # Uncomment and fill in:
        # from prefect.runner.storage import GitRepository
        # from prefect.blocks.system import Secret
        # image=GitRepository(
        #     url="https://github.com/YOUR_ORG/YOUR_REPO.git",
        #     branch="main",
        #     credentials={"access_token": Secret.load("github-token")},
        # ),

        #  Deployment metadata 
        tags=["etl", "snowflake", "mental-health"],
        description="Loads teen mental health data from Excel into Snowflake every 60 minutes.",
    )

    print("\n Deployment registered on Prefect Cloud!")
    print("   → View & trigger runs at: https://app.prefect.io")
    print("   → No local worker needed — default-work-pool is managed by Prefect")