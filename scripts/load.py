import os
import pandas as pd
from sqlalchemy import create_engine, text


DB_USER = os.getenv("POSTGRES_USER", "airflow")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "airflow")
DB_HOST = os.getenv("POSTGRES_HOST", "127.0.0.1")
DB_PORT = os.getenv("POSTGRES_PORT", "5433")
DB_NAME = os.getenv("POSTGRES_DB", "retail_warehouse")

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"


def get_engine():
    return create_engine(DATABASE_URL)


def create_tables(engine):
    with open("sql/create_tables.sql", "r") as file:
        sql = file.read()

    with engine.begin() as connection:
        connection.execute(text(sql))


def load_csv_to_table(engine, csv_path: str, table_name: str):
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"File not found: {csv_path}")

    df = pd.read_csv(csv_path)

    df.to_sql(
        table_name,
        engine,
        if_exists="replace",
        index=False,
    )

    print(f"Loaded {len(df)} rows into table: {table_name}")


def run_load():
    engine = get_engine()

    create_tables(engine)

    load_csv_to_table(
        engine,
        "data/processed/clean_retail_sales.csv",
        "retail_sales",
    )

    load_csv_to_table(
        engine,
        "data/output/country_revenue.csv",
        "country_revenue",
    )

    load_csv_to_table(
        engine,
        "data/output/monthly_revenue.csv",
        "monthly_revenue",
    )

    load_csv_to_table(
        engine,
        "data/output/top_products.csv",
        "top_products",
    )

    print("All tables loaded successfully.")


if __name__ == "__main__":
    run_load()