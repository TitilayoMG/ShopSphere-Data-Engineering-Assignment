
CREATE TABLE customers (
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
CREATE INDEX idx_customers_email ON customers(email);
CREATE INDEX idx_customers_state_city ON customers(state, city);


CREATE TABLE products (
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
CREATE INDEX idx_products_category ON products(category);
CREATE INDEX idx_products_brand ON products(brand);


CREATE TABLE orders (
    order_id INTEGER PRIMARY KEY,
    customer_id INTEGER NOT NULL REFERENCES customers(customer_id),
    order_status VARCHAR(30) NOT NULL,
    order_date TIMESTAMPTZ NOT NULL,
    currency CHAR(3) NOT NULL,
    subtotal NUMERIC(12,2) NOT NULL,
    shipping_fee NUMERIC(12,2) NOT NULL,
    discount_amount NUMERIC(12,2) NOT NULL,
    total_amount NUMERIC(12,2) NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX idx_orders_customer ON orders(customer_id);
CREATE INDEX idx_orders_date ON orders(order_date);
CREATE INDEX idx_orders_status ON orders(order_status);


CREATE TABLE order_items (
    order_item_id INTEGER PRIMARY KEY,
    order_id INTEGER NOT NULL REFERENCES orders(order_id),
    product_id INTEGER NOT NULL REFERENCES products(product_id),
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    unit_price NUMERIC(12,2) NOT NULL,
    discount_amount NUMERIC(12,2) NOT NULL,
    line_total NUMERIC(12,2) NOT NULL
);
CREATE INDEX idx_order_items_order ON order_items(order_id);
CREATE INDEX idx_order_items_product ON order_items(product_id);


CREATE TABLE payments (
    payment_id INTEGER PRIMARY KEY,
    order_id INTEGER NOT NULL REFERENCES orders(order_id),
    payment_method VARCHAR(40) NOT NULL,
    payment_status VARCHAR(30) NOT NULL,
    amount NUMERIC(12,2) NOT NULL,
    transaction_reference VARCHAR(80) NOT NULL UNIQUE,
    paid_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX idx_payments_order ON payments(order_id);
CREATE INDEX idx_payments_status ON payments(payment_status);


--Mongodb customer_sessions
CREATE TABLE customer_sessions (
    _id VARCHAR(24),
    session_id VARCHAR(50),
    customer_id INTEGER,
    started_at TIMESTAMP,
    ended_at TIMESTAMP,
    browser VARCHAR(50),
    location_city VARCHAR(100),
    location_state VARCHAR(100),
    location_country VARCHAR(100),
    device_type VARCHAR(50),
    device_os VARCHAR(100),
    event_type VARCHAR(100),
    event_time TIMESTAMP,
    product_id INTEGER,
    search_term VARCHAR(255),
    quantity INTEGER,
    page_url VARCHAR(255),
    PRIMARY KEY (_id, event_type)
);

CREATE TABLE product_reviews (
    _id                 VARCHAR(24),
    review_id           VARCHAR(50) NOT NULL,
    product_id          INTEGER NOT NULL,
    customer_id         INTEGER NOT NULL,
    rating              INTEGER,
    title               VARCHAR(255),
    review_text         TEXT,
    verified_purchase   BOOLEAN NOT NULL DEFAULT FALSE,
    created_at          TIMESTAMP,
    helpful_votes       INTEGER,
    PRIMARY KEY (_id, product_id)
);