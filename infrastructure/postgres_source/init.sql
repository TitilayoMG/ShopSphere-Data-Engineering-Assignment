CREATE TABLE customers (
    customer_id INTEGER PRIMARY KEY,
    first_name VARCHAR(80) NOT NULL,
    last_name VARCHAR(80) NOT NULL,
    email VARCHAR(160) NOT NULL,
    phone VARCHAR(40),
    city VARCHAR(80) NOT NULL,
    state VARCHAR(80) NOT NULL,
    country VARCHAR(80) NOT NULL DEFAULT 'Nigeria',
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX idx_customers_email ON customers (email);
CREATE INDEX idx_customers_state_city ON customers (state, city);

CREATE TABLE products (
    product_id INTEGER PRIMARY KEY,
    product_name VARCHAR(160) NOT NULL,
    category VARCHAR(80) NOT NULL,
    brand VARCHAR(80),
    unit_price NUMERIC(12, 2) NOT NULL CHECK (unit_price >= 0),
    cost_price NUMERIC(12, 2) NOT NULL CHECK (cost_price >= 0),
    stock_quantity INTEGER NOT NULL CHECK (stock_quantity >= 0),
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX idx_products_category ON products (category);

CREATE TABLE orders (
    order_id INTEGER PRIMARY KEY,
    customer_id INTEGER NOT NULL REFERENCES customers (customer_id),
    order_status VARCHAR(30) NOT NULL CHECK (order_status IN ('pending', 'confirmed', 'processing', 'shipped', 'delivered', 'cancelled', 'returned')),
    order_date TIMESTAMPTZ NOT NULL,
    currency CHAR(3) NOT NULL DEFAULT 'NGN',
    subtotal NUMERIC(12, 2) NOT NULL,
    shipping_fee NUMERIC(12, 2) NOT NULL,
    discount_amount NUMERIC(12, 2) NOT NULL DEFAULT 0,
    total_amount NUMERIC(12, 2) NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX idx_orders_customer_id ON orders (customer_id);
CREATE INDEX idx_orders_order_date ON orders (order_date);
CREATE INDEX idx_orders_updated_at ON orders (updated_at);
CREATE INDEX idx_orders_status ON orders (order_status);

CREATE TABLE order_items (
    order_item_id INTEGER PRIMARY KEY,
    order_id INTEGER NOT NULL REFERENCES orders (order_id),
    product_id INTEGER NOT NULL REFERENCES products (product_id),
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    unit_price NUMERIC(12, 2) NOT NULL,
    discount_amount NUMERIC(12, 2) NOT NULL DEFAULT 0,
    line_total NUMERIC(12, 2) NOT NULL
);
CREATE INDEX idx_order_items_order_id ON order_items (order_id);
CREATE INDEX idx_order_items_product_id ON order_items (product_id);

CREATE TABLE payments (
    payment_id INTEGER PRIMARY KEY,
    order_id INTEGER NOT NULL REFERENCES orders (order_id),
    payment_method VARCHAR(40) NOT NULL,
    payment_status VARCHAR(30) NOT NULL CHECK (payment_status IN ('pending', 'successful', 'failed', 'refunded')),
    amount NUMERIC(12, 2) NOT NULL,
    transaction_reference VARCHAR(80) NOT NULL UNIQUE,
    paid_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX idx_payments_order_id ON payments (order_id);
CREATE INDEX idx_payments_status ON payments (payment_status);
CREATE INDEX idx_payments_updated_at ON payments (updated_at);
