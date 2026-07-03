# Warehouse Requirements

The warehouse database is initialized with these schemas:

- `staging`
- `analytics`
- `control`

Only control tables are provided. You must design and create the analytical model.

## Required Control Tables

- `control.pipeline_runs`
- `control.pipeline_watermarks`

Use these tables to record pipeline execution status, row counts, errors, and incremental-load watermarks.

## Suggested Analytical Tables

Design and create tables such as:

- `analytics.dim_customer`
- `analytics.dim_product`
- `analytics.dim_date`
- `analytics.dim_carrier`
- `analytics.fact_orders`
- `analytics.fact_order_items`
- `analytics.fact_payments`
- `analytics.fact_customer_events`
- `analytics.fact_shipments`
- `analytics.fact_shipment_events`
- `analytics.fact_product_reviews`

## Business Questions To Support

- Daily and monthly sales
- Revenue by product and category
- Average order value
- Payment success rate
- Customer purchase frequency
- Product-view-to-purchase conversion
- Cart abandonment
- Average review score
- Average delivery time
- Late-delivery rate
- Orders without shipments
- Delivered shipments linked to cancelled orders
- Repeat customers
- Sales by city and state

Document your table grain, keys, deduplication strategy, and assumptions.
