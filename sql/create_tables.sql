CREATE TABLE IF NOT EXISTS retail_sales (
    invoice_no VARCHAR(50),
    stock_code VARCHAR(50),
    description TEXT,
    quantity INTEGER,
    invoice_date TIMESTAMP,
    unit_price NUMERIC(10, 2),
    customer_id INTEGER,
    country VARCHAR(100),
    total_price NUMERIC(12, 2),
    invoice_date_only DATE,
    year INTEGER,
    month INTEGER
);

CREATE TABLE IF NOT EXISTS country_revenue (
    country VARCHAR(100),
    total_revenue NUMERIC(14, 2),
    total_orders INTEGER,
    total_customers INTEGER
);

CREATE TABLE IF NOT EXISTS monthly_revenue (
    year_month VARCHAR(20),
    total_revenue NUMERIC(14, 2),
    total_orders INTEGER
);

CREATE TABLE IF NOT EXISTS top_products (
    stock_code VARCHAR(50),
    description TEXT,
    total_quantity INTEGER,
    total_revenue NUMERIC(14, 2)
);