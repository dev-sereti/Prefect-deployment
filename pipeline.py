import hashlib
import io
import os
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv
from prefect import flow, task, get_run_logger
from prefect.blocks.system import Secret
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine, text


# 
# CONFIG
# 

BASE_DIR = Path(__file__).parent

# SharePoint / OneDrive Excel URL
EXCEL_FILE_URL = (
    "https://victoryfarmsltd-my.sharepoint.com/"
    "personal/kelvins_victoryfarmskenya_com/"
    "Documents/Teen%20Mental%20Health/"
    "Teen_Mental_Health_Dataset.xlsx"
)

TABLE = "TEEN_MENTAL_HEALTH"

# Local development fallback
load_dotenv(BASE_DIR / ".env")


# 
# SECRET LOADER
# 

def _load_secret(secret_name: str, env_key: str) -> str:
    """
    Credential priority:
      1. Prefect Secret Block
      2. Environment variable / .env
    """

    # Prefect Secret block
    try:
        return Secret.load(secret_name).get()
    except Exception:
        pass

    # .env fallback
    value = os.getenv(env_key)

    if value:
        return value

    raise ValueError(
        f"""
Missing credential.

Prefect Secret Block:
    {secret_name}

Environment Variable:
    {env_key}

Fix:
1. Create Prefect Secret block
OR
2. Add the variable to .env
"""
    )


# 
# DATABASE ENGINE
# 

def get_engine():
    """
    Create Snowflake SQLAlchemy engine.

    NOTE:
    Not decorated as a task to avoid
    Prefect serialization issues.
    """

    logger = get_run_logger()

    logger.info("Loading Snowflake credentials...")

    url = URL(
        account=_load_secret("snowflake-account", "SNOWFLAKE_ACCOUNT"),
        user=_load_secret("snowflake-user", "SNOWFLAKE_USER"),
        password=_load_secret("snowflake-password", "SNOWFLAKE_PASSWORD"),
        database=_load_secret("snowflake-database", "SNOWFLAKE_DATABASE"),
        schema=_load_secret("snowflake-schema", "SNOWFLAKE_SCHEMA"),
        warehouse=_load_secret("snowflake-warehouse", "SNOWFLAKE_WAREHOUSE"),
        role=_load_secret("snowflake-role", "SNOWFLAKE_ROLE"),
    )

    engine = create_engine(url)

    logger.info("Snowflake engine created successfully")

    return engine


# 
# EXTRACT
# 

@task(
    name="Extract — Download Excel from SharePoint",
    retries=2,
    retry_delay_seconds=15
)
def extract(url: str) -> pd.DataFrame:

    logger = get_run_logger()

    logger.info("Downloading Excel file from SharePoint...")

    response = requests.get(url, timeout=120)

    if response.status_code != 200:
        raise ValueError(
            f"""
Failed to download Excel file.

Status Code: {response.status_code}

URL:
{url}
"""
        )

    logger.info("Excel file downloaded successfully")

    excel_bytes = io.BytesIO(response.content)

    df = pd.read_excel(
        excel_bytes,
        sheet_name="Teen_Mental_Health_Dataset",
        engine="openpyxl"
    )

    logger.info(f"{len(df):,} rows extracted")

    return df


# 
# TRANSFORM
# 

@task(name="Transform — Clean & Enrich")
def transform(df: pd.DataFrame) -> pd.DataFrame:

    logger = get_run_logger()

    logger.info("Starting transformation phase...")

    # Normalize text columns
    text_columns = [
        "gender",
        "platform_usage",
        "social_interaction_level"
    ]

    for col in text_columns:
        if col in df.columns:
            df[col] = (
                df[col]
                .astype(str)
                .str.strip()
                .str.lower()
            )

    # Numeric columns
    numeric_columns = [
        "daily_social_media_hours",
        "sleep_hours",
        "screen_time_before_sleep",
        "academic_performance",
        "physical_activity",
        "stress_level",
        "anxiety_level",
        "addiction_level",
        "depression_label",
        "age"
    ]

    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(
                df[col],
                errors="coerce"
            ).round(2)

    # Create age groups
    df["age_group"] = pd.cut(
        df["age"],
        bins=[13, 15, 17, 19],
        labels=["14-15", "16-17", "18-19"]
    ).astype(str)

    # Remove duplicates
    before = len(df)

    df = df.drop_duplicates()

    removed = before - len(df)

    if removed > 0:
        logger.warning(
            f"Removed {removed:,} duplicate rows"
        )

    # Create row hash
    logger.info("Generating row hashes...")

    df["row_hash"] = df.apply(
        lambda row: hashlib.sha256(
            "|".join(str(v) for v in row.values).encode()
        ).hexdigest(),
        axis=1
    )

    logger.info(
        f"Transformation complete: "
        f"{len(df):,} rows ready"
    )

    return df


# 
# CREATE TABLE
# 

@task(
    name="Init — Create Table",
    retries=2,
    retry_delay_seconds=15
)
def create_table_if_not_exists():

    logger = get_run_logger()

    engine = get_engine()

    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {TABLE} (
        ID                        NUMBER AUTOINCREMENT PRIMARY KEY,
        AGE                       NUMBER(5,0),
        GENDER                    VARCHAR(50),
        DAILY_SOCIAL_MEDIA_HOURS  NUMBER(4,2),
        PLATFORM_USAGE            VARCHAR(100),
        SLEEP_HOURS               NUMBER(4,2),
        SCREEN_TIME_BEFORE_SLEEP  NUMBER(4,2),
        ACADEMIC_PERFORMANCE      NUMBER(5,2),
        PHYSICAL_ACTIVITY         NUMBER(4,2),
        SOCIAL_INTERACTION_LEVEL  VARCHAR(50),
        STRESS_LEVEL              NUMBER(5,0),
        ANXIETY_LEVEL             NUMBER(5,0),
        ADDICTION_LEVEL           NUMBER(5,0),
        DEPRESSION_LABEL          NUMBER(5,0),
        AGE_GROUP                 VARCHAR(10),
        ROW_HASH                  VARCHAR(64) UNIQUE,
        IMPORTED_AT               TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
    );
    """

    with engine.begin() as conn:
        conn.execute(text(create_sql))

    logger.info(f"Table {TABLE} is ready")


# 
# FETCH EXISTING HASHES
# 

@task(
    name="Incremental — Fetch Existing Hashes",
    retries=2,
    retry_delay_seconds=10
)
def get_existing_hashes() -> set:

    logger = get_run_logger()

    engine = get_engine()

    with engine.connect() as conn:

        result = conn.execute(
            text(f"SELECT ROW_HASH FROM {TABLE}")
        )

        existing_hashes = {
            row[0]
            for row in result
        }

    logger.info(
        f"{len(existing_hashes):,} existing hashes fetched"
    )

    return existing_hashes


# 
# FILTER NEW ROWS
# 

@task(name="Incremental — Filter New Rows")
def filter_new_rows(
    df: pd.DataFrame,
    existing_hashes: set
) -> pd.DataFrame:

    logger = get_run_logger()

    new_df = df[
        ~df["row_hash"].isin(existing_hashes)
    ].copy()

    skipped = len(df) - len(new_df)

    logger.info(
        f"{skipped:,} rows skipped | "
        f"{len(new_df):,} new rows ready"
    )

    return new_df


# 
# LOAD TO SNOWFLAKE
# 

@task(
    name="Load — Append New Rows",
    retries=2,
    retry_delay_seconds=15
)
def load_to_snowflake(df: pd.DataFrame):

    logger = get_run_logger()

    if df.empty:
        logger.info(
            "No new rows to load. "
            "Snowflake is already up to date."
        )
        return

    engine = get_engine()

    # Snowflake naming convention
    df.columns = [col.upper() for col in df.columns]

    df.to_sql(
        TABLE.lower(),
        engine,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=5000
    )

    logger.info(
        f"Successfully appended "
        f"{len(df):,} new rows into {TABLE}"
    )


# 
# FLOW
# 

@flow(
    name="Teen Mental Health — Incremental Excel to Snowflake",
    description=(
        "Incrementally loads new rows "
        "from SharePoint Excel into Snowflake "
        "using row hashing."
    ),
    log_prints=True
)
def teen_mental_health_pipeline():

    logger = get_run_logger()

    logger.info("Pipeline execution started")

    # Extract
    df = extract(EXCEL_FILE_URL)

    # Transform
    df = transform(df)

    # Ensure table exists
    create_table_if_not_exists()

    # Get existing hashes
    existing_hashes = get_existing_hashes()

    # Incremental filter
    new_df = filter_new_rows(
        df,
        existing_hashes
    )

    # Load
    load_to_snowflake(new_df)

    logger.info(
        "Pipeline execution completed successfully"
    )


# 
# LOCAL ENTRYPOINT
# 

if __name__ == "__main__":

    teen_mental_health_pipeline.serve(
        name="teen-mental-health-etl",
        cron="0 * * * *",  # every hour
        tags=["snowflake", "etl", "excel", "sharepoint"],
        description=(
            "Loads teen mental health data "
            "from SharePoint Excel into Snowflake "
            "every hour."
        )
    )