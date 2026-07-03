INSERT INTO customers (customer_id, first_name, last_name, email, phone, city, state, country, created_at, updated_at)
SELECT customer_id,
    (ARRAY['Ada','Tunde','Chioma','Emeka','Amina','Sola','Ifeoma','Musa','Bisi','Kunle'])[1 + ((customer_id - 1) % 10)],
    (ARRAY['Okafor','Adeyemi','Eze','Ibrahim','Balogun','Nwosu','Garba','Ojo','Adebayo','Umeh'])[1 + ((customer_id - 1) % 10)],
    CASE WHEN customer_id IN (12, 88) THEN 'duplicate.customer@shopsphere.test' ELSE 'customer' || LPAD(customer_id::text, 3, '0') || '@example.com' END,
    CASE WHEN customer_id IN (15, 47, 92) THEN NULL ELSE '+23480' || LPAD((70000000 + customer_id * 371)::text, 8, '0') END,
    CASE WHEN customer_id IN (23, 71) THEN LOWER((ARRAY['Lagos','Abuja','Port Harcourt','Ibadan','Kano','Enugu','Benin City','Abeokuta'])[1 + ((customer_id - 1) % 8)]) ELSE (ARRAY['Lagos','Abuja','Port Harcourt','Ibadan','Kano','Enugu','Benin City','Abeokuta'])[1 + ((customer_id - 1) % 8)] END,
    (ARRAY['Lagos','FCT','Rivers','Oyo','Kano','Enugu','Edo','Ogun'])[1 + ((customer_id - 1) % 8)],
    'Nigeria',
    TIMESTAMPTZ '2025-01-01 08:00:00+00' + (customer_id || ' days')::interval,
    TIMESTAMPTZ '2025-01-01 08:00:00+00' + ((customer_id + (customer_id % 7)) || ' days')::interval
FROM generate_series(1, 120) AS customer_id;

INSERT INTO products (product_id, product_name, category, brand, unit_price, cost_price, stock_quantity, created_at, updated_at)
SELECT product_id,
    (ARRAY['Smartphone','Laptop','Headphones','Blender','Sneakers','Backpack','Desk Lamp','Power Bank','Rice Cooker','T-Shirt'])[1 + ((product_id - 1) % 10)] || ' ' || product_id,
    (ARRAY['Electronics','Computing','Audio','Home Appliances','Fashion','Accessories','Home Office','Electronics','Kitchen','Fashion'])[1 + ((product_id - 1) % 10)],
    CASE WHEN product_id IN (7, 42) THEN NULL ELSE (ARRAY['NovaTech','KongaBasics','AfriHome','SwiftWear','Luma','UrbanKit'])[1 + ((product_id - 1) % 6)] END,
    (2500 + product_id * 850 + (product_id % 5) * 125)::numeric(12, 2),
    (1500 + product_id * 520 + (product_id % 5) * 80)::numeric(12, 2),
    20 + (product_id * 3 % 180),
    TIMESTAMPTZ '2025-01-01 00:00:00+00' + (product_id || ' days')::interval,
    TIMESTAMPTZ '2025-02-01 00:00:00+00' + ((product_id % 20) || ' days')::interval
FROM generate_series(1, 60) AS product_id;

WITH order_base AS (
    SELECT order_id,
        1 + ((order_id * 7) % 120) AS customer_id,
        (ARRAY['pending','confirmed','processing','shipped','delivered','cancelled','returned'])[1 + ((order_id - 1) % 7)] AS order_status,
        TIMESTAMPTZ '2025-03-01 09:00:00+00' + (order_id || ' hours')::interval AS order_date,
        (15000 + (order_id % 35) * 1250)::numeric(12, 2) AS subtotal,
        (1500 + (order_id % 4) * 500)::numeric(12, 2) AS shipping_fee,
        CASE WHEN order_id % 9 = 0 THEN 2000 ELSE 0 END::numeric(12, 2) AS discount_amount
    FROM generate_series(1, 320) AS order_id
)
INSERT INTO orders (order_id, customer_id, order_status, order_date, currency, subtotal, shipping_fee, discount_amount, total_amount, updated_at)
SELECT order_id, customer_id, order_status, order_date, 'NGN', subtotal, shipping_fee, discount_amount,
       subtotal + shipping_fee - discount_amount,
       order_date + ((1 + (order_id % 11)) || ' hours')::interval
FROM order_base;

INSERT INTO order_items (order_item_id, order_id, product_id, quantity, unit_price, discount_amount, line_total)
SELECT ((order_id - 1) * 2) + item_no,
    order_id,
    1 + (((order_id * 3) + item_no) % 60),
    1 + ((order_id + item_no) % 3),
    (2500 + (1 + (((order_id * 3) + item_no) % 60)) * 850 + ((1 + (((order_id * 3) + item_no) % 60)) % 5) * 125)::numeric(12, 2),
    CASE WHEN (order_id + item_no) % 12 = 0 THEN 500 ELSE 0 END::numeric(12, 2),
    ((1 + ((order_id + item_no) % 3)) * (2500 + (1 + (((order_id * 3) + item_no) % 60)) * 850 + ((1 + (((order_id * 3) + item_no) % 60)) % 5) * 125) - CASE WHEN (order_id + item_no) % 12 = 0 THEN 500 ELSE 0 END)::numeric(12, 2)
FROM generate_series(1, 320) AS order_id
CROSS JOIN generate_series(1, 2) AS item_no;

INSERT INTO payments (payment_id, order_id, payment_method, payment_status, amount, transaction_reference, paid_at, updated_at)
SELECT payment_id,
    payment_id,
    (ARRAY['card','bank_transfer','ussd','wallet'])[1 + ((payment_id - 1) % 4)],
    CASE WHEN payment_id % 17 = 0 THEN 'failed' WHEN payment_id % 19 = 0 THEN 'refunded' WHEN payment_id % 13 = 0 THEN 'pending' ELSE 'successful' END,
    CASE WHEN payment_id IN (18, 149, 222) THEN o.total_amount + 750 WHEN payment_id IN (57, 211) THEN o.total_amount - 500 ELSE o.total_amount END,
    'TXN-SHOP-' || LPAD(payment_id::text, 5, '0'),
    CASE WHEN payment_id % 13 = 0 THEN NULL ELSE o.order_date + ((payment_id % 6) || ' hours')::interval END,
    o.updated_at + ((payment_id % 5) || ' hours')::interval
FROM generate_series(1, 280) AS payment_id
JOIN orders o ON o.order_id = payment_id;
