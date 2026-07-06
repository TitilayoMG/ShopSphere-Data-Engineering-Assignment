
CREATE TABLE dim_customers (
    customer_id INTEGER PRIMARY KEY,
    first_name VARCHAR(80) NOT NULL,
    last_name VARCHAR(80) NOT NULL,
    email VARCHAR(160) NOT NULL,
    phone VARCHAR(40),
    city VARCHAR(80) NOT NULL,
    state VARCHAR(80) NOT NULL,
    country VARCHAR(80) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX idx_dim_customers_email ON dim_customers(email);
CREATE INDEX idx_dim_customers_state_city ON dim_customers(state, city);


CREATE TABLE dim_products (
    product_id INTEGER PRIMARY KEY,
    product_name VARCHAR(160) NOT NULL,
    category VARCHAR(80) NOT NULL,
    brand VARCHAR(80) NOT NULL,
    unit_price NUMERIC(12,2) NOT NULL CHECK (unit_price >= 0),
    cost_price NUMERIC(12,2) NOT NULL CHECK (cost_price >= 0),
    stock_quantity INTEGER NOT NULL CHECK (stock_quantity >= 0),
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX idx_dim_products_category ON dim_products(category);
CREATE INDEX idx_dim_products_brand ON dim_products(brand);


CREATE TABLE fact_orders (
    order_id INTEGER PRIMARY KEY,
    customer_id INTEGER NOT NULL REFERENCES dim_customers(customer_id),
    order_status VARCHAR(30) NOT NULL,
    order_date TIMESTAMPTZ NOT NULL,
    currency CHAR(3) NOT NULL,
    subtotal NUMERIC(12,2) NOT NULL,
    shipping_fee NUMERIC(12,2) NOT NULL,
    discount_amount NUMERIC(12,2) NOT NULL,
    total_amount NUMERIC(12,2) NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX idx_fact_orders_customer ON fact_orders(customer_id);
CREATE INDEX idx_fact_orders_date ON fact_orders(order_date);
CREATE INDEX idx_fact_orders_status ON fact_orders(order_status);


CREATE TABLE fact_order_items (
    order_item_id INTEGER PRIMARY KEY,
    order_id INTEGER NOT NULL REFERENCES fact_orders(order_id),
    product_id INTEGER NOT NULL REFERENCES dim_products(product_id),
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    unit_price NUMERIC(12,2) NOT NULL,
    discount_amount NUMERIC(12,2) NOT NULL,
    line_total NUMERIC(12,2) NOT NULL
);
CREATE INDEX idx_fact_order_items_order ON fact_order_items(order_id);
CREATE INDEX idx_fact_order_items_product ON fact_order_items(product_id);


CREATE TABLE fact_payments (
    payment_id INTEGER PRIMARY KEY,
    order_id INTEGER NOT NULL REFERENCES fact_orders(order_id),
    payment_method VARCHAR(40) NOT NULL,
    payment_status VARCHAR(30) NOT NULL,
    amount NUMERIC(12,2) NOT NULL,
    transaction_reference VARCHAR(80) NOT NULL UNIQUE,
    paid_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX idx_fact_payments_order ON fact_payments(order_id);
CREATE INDEX idx_fact_payments_status ON fact_payments(payment_status);
