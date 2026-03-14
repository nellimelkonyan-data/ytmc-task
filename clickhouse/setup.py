import clickhouse_connect
from pathlib import Path
from dotenv import load_dotenv
import os 

# ClickHouse connection
load_dotenv()

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD")

client = clickhouse_connect.get_client(
    host=CLICKHOUSE_HOST,
    port=CLICKHOUSE_PORT,
    username=CLICKHOUSE_USER,
    password=CLICKHOUSE_PASSWORD,
)

# Folder containing DDL scripts
DDL_DIR = Path(__file__).parent / "ddl"


def run_sql_file(file_path):
    print(f"Running {file_path.name}...")

    sql = file_path.read_text()

    # split statements by ;
    statements = [s.strip() for s in sql.split(";") if s.strip()]

    for stmt in statements:
        client.command(stmt)

    print(f"✓ {file_path.name} executed")


def main():
    sql_files = sorted(DDL_DIR.glob("*.sql"))

    if not sql_files:
        print("No SQL files found")
        return

    for file in sql_files:
        run_sql_file(file)

    print("\nAll DDL scripts executed successfully 🚀")


if __name__ == "__main__":
    main()