import os
import re
from pathlib import Path
from urllib.parse import urlparse, parse_qs

import clickhouse_connect
import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv()

RAW_DIR = Path(__file__).parent.parent / "data" / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)

REQUIRED_ENV_VARS = [
    "CLICKHOUSE_HOST",
    "CLICKHOUSE_PORT",
    "CLICKHOUSE_USER",
    "CLICKHOUSE_PASSWORD",
    "ACTIVITY_CSV_URL",
    "ACTIVITY_TYPES_CSV_URL",
    "DEAL_CHANGES_CSV_URL",
    "FIELDS_CSV_URL",
    "STAGES_CSV_URL",
    "USERS_CSV_URL",
]

for var in REQUIRED_ENV_VARS:
    if not os.getenv(var):
        raise ValueError(f"Missing required environment variable: {var}")

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD")

GOOGLE_DRIVE_FILES = {
    "activity.csv": os.getenv("ACTIVITY_CSV_URL"),
    "activity_types.csv": os.getenv("ACTIVITY_TYPES_CSV_URL"),
    "deal_changes.csv": os.getenv("DEAL_CHANGES_CSV_URL"),
    "fields.csv": os.getenv("FIELDS_CSV_URL"),
    "stages.csv": os.getenv("STAGES_CSV_URL"),
    "users.csv": os.getenv("USERS_CSV_URL"),
}

client = clickhouse_connect.get_client(
    host=CLICKHOUSE_HOST,
    port=CLICKHOUSE_PORT,
    username=CLICKHOUSE_USER,
    password=CLICKHOUSE_PASSWORD,
)


def extract_google_drive_file_id(url: str) -> str:
    """
    Extract Google Drive file ID from common public URL formats.
    Supported examples:
    - https://drive.google.com/file/d/<FILE_ID>/view?usp=sharing
    - https://drive.google.com/open?id=<FILE_ID>
    - https://drive.google.com/uc?export=download&id=<FILE_ID>
    """
    parsed = urlparse(url)

    match = re.search(r"/file/d/([^/]+)", parsed.path)
    if match:
        return match.group(1)

    query_params = parse_qs(parsed.query)
    if "id" in query_params:
        return query_params["id"][0]

    raise ValueError(f"Could not extract Google Drive file ID from URL: {url}")


def build_google_drive_download_url(url: str) -> str:
    """
    Convert a public Google Drive sharing URL into a direct download URL.
    """
    if "uc?export=download" in url:
        return url

    file_id = extract_google_drive_file_id(url)
    return f"https://drive.google.com/uc?export=download&id={file_id}"


def download_file(url: str, output_path: Path) -> None:
    """
    Download a file from a public Google Drive link.
    """
    download_url = build_google_drive_download_url(url)
    response = requests.get(download_url, stream=True, timeout=60)
    response.raise_for_status()

    with open(output_path, "wb") as file:
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                file.write(chunk)


def to_snake_case(name: str) -> str:
    """
    Convert a column name to lowercase snake_case.
    """
    name = name.strip()
    name = re.sub(r"[^\w\s-]", "", name)
    name = re.sub(r"[\s-]+", "_", name)
    return name.lower()


def normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardize CSV column names and basic values before Bronze load.
    """
    df.columns = [to_snake_case(col) for col in df.columns]

    for col in df.columns:
        if df[col].dtype == "object":
            df[col] = df[col].fillna("")

    return df


def load_csv_to_clickhouse(file_path: Path, table_name: str) -> None:
    """
    Load one CSV file into one Bronze table.
    """
    print(f"Loading {file_path.name} into crm_raw.{table_name}")

    df = pd.read_csv(file_path)
    # Standardize column names: lowercase + snake_case
    df.columns = [to_snake_case(col) for col in df.columns]

    # Convert date/time columns if present
    for col in df.columns:
        if "time" in col or "date" in col or col in ["modified", "due_to", "change_time"]:
            try:
                df[col] = pd.to_datetime(df[col])
            except Exception:
                pass  # keep original if parsing fails

    # Full refresh bronze behavior for now
    client.command(f"TRUNCATE TABLE crm_raw.{table_name}")

    client.insert_df(
        table=f"crm_raw.{table_name}",
        df=df,
    )

    print(f"Loaded {len(df)} rows into crm_raw.{table_name}")


def main() -> None:
    for file_name, public_link in GOOGLE_DRIVE_FILES.items():
        local_file_path = RAW_DIR / file_name

        print(f"Downloading {file_name} from Google Drive...")
        download_file(public_link, local_file_path)

        table_name = file_name.replace(".csv", "")
        load_csv_to_clickhouse(local_file_path, table_name)


if __name__ == "__main__":
    main()