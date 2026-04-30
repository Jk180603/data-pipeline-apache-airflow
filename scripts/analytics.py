import os
import pandas as pd


PROCESSED_PATH = "data/processed/clean_retail_sales.csv"
OUTPUT_DIR = "data/output"


def load_clean_data(path: str = PROCESSED_PATH) -> pd.DataFrame:
    if not os.path.exists(path):
        raise FileNotFoundError(f"Processed file not found at: {path}")

    return pd.read_csv(path, parse_dates=["invoice_date"])


def generate_country_revenue(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.groupby("country", as_index=False)
        .agg(
            total_revenue=("total_price", "sum"),
            total_orders=("invoice_no", "nunique"),
            total_customers=("customer_id", "nunique"),
        )
        .sort_values("total_revenue", ascending=False)
    )


def generate_monthly_revenue(df: pd.DataFrame) -> pd.DataFrame:
    df["year_month"] = df["invoice_date"].dt.to_period("M").astype(str)

    return (
        df.groupby("year_month", as_index=False)
        .agg(
            total_revenue=("total_price", "sum"),
            total_orders=("invoice_no", "nunique"),
        )
        .sort_values("year_month")
    )


def generate_top_products(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.groupby(["stock_code", "description"], as_index=False)
        .agg(
            total_quantity=("quantity", "sum"),
            total_revenue=("total_price", "sum"),
        )
        .sort_values("total_revenue", ascending=False)
        .head(20)
    )


def run_analytics() -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    df = load_clean_data()

    country_revenue = generate_country_revenue(df)
    monthly_revenue = generate_monthly_revenue(df)
    top_products = generate_top_products(df)

    country_revenue.to_csv(f"{OUTPUT_DIR}/country_revenue.csv", index=False)
    monthly_revenue.to_csv(f"{OUTPUT_DIR}/monthly_revenue.csv", index=False)
    top_products.to_csv(f"{OUTPUT_DIR}/top_products.csv", index=False)

    print("Analytics outputs generated:")
    print(f"- {OUTPUT_DIR}/country_revenue.csv")
    print(f"- {OUTPUT_DIR}/monthly_revenue.csv")
    print(f"- {OUTPUT_DIR}/top_products.csv")


if __name__ == "__main__":
    run_analytics()