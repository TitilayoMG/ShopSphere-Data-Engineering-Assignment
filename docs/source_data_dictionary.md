# Source Data Dictionary

## PostgreSQL: `shopsphere`

### `customers`

Customer profile records with contact details, location fields, and creation/update timestamps.

### `products`

Product catalog records with category, brand, price, cost, stock, and timestamp fields.

### `orders`

Transactional order headers with order status, amounts, order date, and update timestamp. Orders cover several months and multiple Nigerian cities through customer links.

### `order_items`

Line-level order details linked to orders and products.

### `payments`

Payment attempts linked to orders, including payment method, status, amount, transaction reference, and timestamps.

## MongoDB: `shopsphere_events`

### `customer_sessions`

Website session documents with nested device and location fields plus an `events` array.

### `product_reviews`

Product review documents with ratings, review text, purchase verification, timestamps, and helpful vote counts.

## SwiftDrop Logistics API

### `/api/v1/carriers`

Returns logistics carriers used by shipments.

### `/api/v1/shipments`

Returns paginated shipment records with nested delivery addresses and shipment status events.
