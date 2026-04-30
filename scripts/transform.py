import os
import pandas as pd


RAW_PATH = "data/raw/Online Retail.xlsx"
PROCESSED_PATH = "data/processed/clean_retail_sales.csv"


def load_raw_data(path: str = RAW_PATH) -> pd.DataFrame:
    if not os.path.exists(path):
        raise FileNotFoundError(f"Raw dataset not found at: {path}")

    df = pd.read_excel(path)
    return df


def clean_retail_data(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df.columns = [
        "invoice_no",
        "stock_code",
        "description",
        "quantity",
        "invoice_date",
        "unit_price",
        "customer_id",
        "country",
    ]

    df = df.dropna(subset=["customer_id"])
    df = df[df["quantity"] > 0]
    df = df[df["unit_price"] > 0]

    df["customer_id"] = df["customer_id"].astype(int)
    df["invoice_date"] = pd.to_datetime(df["invoice_date"])
    df["total_price"] = df["quantity"] * df["unit_price"]

    df["invoice_date_only"] = df["invoice_date"].dt.date
    df["year"] = df["invoice_date"].dt.year
    df["month"] = df["invoice_date"].dt.month

    return df


def save_processed_data(df: pd.DataFrame, path: str = PROCESSED_PATH) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_csv(path, index=False)


def run_transform() -> None:
    df = load_raw_data()
    clean_df = clean_retail_data(df)
    save_processed_data(clean_df)

    print(f"Raw shape: {df.shape}")
    print(f"Clean shape: {clean_df.shape}")
    print(f"Saved processed file to: {PROCESSED_PATH}")


if __name__ == "__main__":
    run_transform()