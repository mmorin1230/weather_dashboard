from src.extract import save_bronze_snapshot
from src.transform import load_latest_bronze_snapshot, hourly_to_dataframe
from src.load import ensure_schema, upsert_silver
from src.gold import ensure_gold_schema, upsert_gold  # if you moved gold logic to code


def run(city: str):
    print("Extracting...")
    path = save_bronze_snapshot(city)

    print("Transforming...")
    payload = load_latest_bronze_snapshot(city)
    df = hourly_to_dataframe(payload)

    print("Loading Silver...")
    ensure_schema()
    upsert_silver(df)

    print("Aggregating Gold...")
    ensure_gold_schema()
    upsert_gold()

    print("Done.")


if __name__ == "__main__":
    run("San Jose")