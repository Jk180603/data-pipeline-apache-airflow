import os
import pandas as pd


PROCESSED_PATH = "data/processed/clean_retail_sales.csv"


def run_quality_checks(path: str = PROCESSED_PATH) -> None:
    if not os.path.exists(path):
        raise FileNotFoundError(f"Processed file not found at: {path}")

    df = pd.read_csv(path)

    required_columns = [
        "invoice_no",
        "stock_code",
        "description",
        "quantity",
        "invoice_date",
        "unit_price",
        "customer_id",
        "country",
        "total_price",
        "invoice_date_only",
        "year",
        "month",
    ]

    missing_columns = [col for col in required_columns if col not in df.columns]

    if missing_columns:
        raise ValueError(f"Missing columns: {missing_columns}")

    if df.empty:
        raise ValueError("Data quality failed: dataframe is empty")

    if df["customer_id"].isnull().any():
        raise ValueError("Data quality failed: customer_id contains null values")

    if (df["quantity"] <= 0).any():
        raise ValueError("Data quality failed: quantity contains non-positive values")

    if (df["unit_price"] <= 0).any():
        raise ValueError("Data quality failed: unit_price contains non-positive values")

    if (df["total_price"] <= 0).any():
        raise ValueError("Data quality failed: total_price contains non-positive values")

    print("Data quality checks passed.")
    print(f"Rows checked: {len(df)}")
    print(f"Columns checked: {len(df.columns)}")


if __name__ == "__main__":
    run_quality_checks()