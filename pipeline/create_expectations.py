import great_expectations as gx

context = gx.get_context()

suite = context.suites.add("customer_sessions")

# add expectations...

# Primary identifiers
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToNotBeNull(
        column="session_id"
    )
)

suite.add_expectation(
    gx.expectations.ExpectColumnValuesToNotBeNull(
        column="customer_id"
    )
)

# Session timestamps
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToNotBeNull(
        column="started_at"
    )
)

suite.add_expectation(
    gx.expectations.ExpectColumnValuesToNotBeNull(
        column="ended_at"
    )
)

suite.add_expectation(
    gx.expectations.ExpectColumnPairValuesAToBeLessThanB(
        column_A="started_at",
        column_B="ended_at",
        or_equal=True,
    )
)

# Event timestamp
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToNotBeNull(
        column="event_time"
    )
)

# Device information
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToNotBeNull(
        column="device_type"
    )
)

suite.add_expectation(
    gx.expectations.ExpectColumnValuesToNotBeNull(
        column="device_os"
    )
)

# Event type
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToNotBeNull(
        column="event_type"
    )
)

# Customer IDs
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToBeOfType(
        column="customer_id",
        type_="int64"
    )
)

# Product IDs (nullable)
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToBeOfType(
        column="product_id",
        type_="int64"
    )
)

# Quantity (nullable)
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToBeBetween(
        column="quantity",
        min_value=1,
    )
)

# No duplicate rows after transformation
suite.add_expectation(
    gx.expectations.ExpectCompoundColumnsToBeUnique(
        column_list=[
            "session_id",
            "event_time",
            "event_type",
            "product_id",
        ]
    )
)


context.suites.save(suite)