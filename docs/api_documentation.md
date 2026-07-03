# SwiftDrop Logistics API Documentation

The mock API runs at `http://localhost:8000` after Docker Compose starts.

Interactive OpenAPI documentation is available at:

```text
http://localhost:8000/docs
```

## Health

`GET /health`

Response:

```json
{"status": "healthy"}
```

## Carriers

`GET /api/v1/carriers`

Returns all carrier records.

## Shipments

`GET /api/v1/shipments`

Query parameters:

- `page`: page number, starting at 1
- `limit`: page size, maximum 100
- `status`: optional shipment status filter
- `updated_since`: optional ISO-8601 datetime filter using `updated_at`

Response fields:

- `page`
- `limit`
- `total`
- `total_pages`
- `next_page`
- `shipments`

## Shipment Detail

`GET /api/v1/shipments/{shipment_id}`

Returns one shipment or HTTP 404 when the shipment does not exist.

## Optional Extension Ideas

The API intentionally does not implement authentication, rate limits, or random failures. You may add client-side support for these production concerns as bonus work:

- Retry transient failures
- Back off after HTTP 429 responses
- Add API token configuration
- Persist extraction checkpoints per page or watermark
