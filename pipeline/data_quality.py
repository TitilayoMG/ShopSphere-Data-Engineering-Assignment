import great_expectations as gx


def validate_customer_sessions(df):
    """
    Validate transformed customer_sessions dataframe.

    Raises:
        ValueError: If any expectation fails.
    """

    validator = gx.from_pandas(df)

    validations = [

        validator.expect_column_values_to_not_be_null(
            "session_id"
        ),

        validator.expect_column_values_to_not_be_null(
            "customer_id"
        ),

        validator.expect_column_values_to_not_be_null(
            "started_at"
        ),

        validator.expect_column_values_to_not_be_null(
            "ended_at"
        ),

        validator.expect_column_values_to_not_be_null(
            "event_time"
        ),

        validator.expect_column_values_to_not_be_null(
            "device_type"
        ),

        validator.expect_column_values_to_not_be_null(
            "device_os"
        ),

        validator.expect_column_values_to_not_be_null(
            "event_type"
        ),

        validator.expect_column_pair_values_a_to_be_less_than_b(
            column_A="started_at",
            column_B="ended_at",
            or_equal=True,
        ),

        validator.expect_column_values_to_be_of_type(
            "customer_id",
            "Int64",
        ),

        validator.expect_column_values_to_be_of_type(
            "product_id",
            "Int64",
        ),

        validator.expect_column_values_to_be_between(
            "quantity",
            min_value=1,
            mostly=1.0,
            ignore_row_if="either_value_is_missing",
        ),

        validator.expect_compound_columns_to_be_unique(
            [
                "session_id",
                "event_time",
                "event_type",
                "product_id",
            ]
        ),
    ]

    failed = [
        result
        for result in validations
        if not result["success"]
    ]

    if failed:
        messages = []

        for result in failed:
            messages.append(
                f"{result['expectation_config']['expectation_type']} failed"
            )

        raise ValueError(
            "Customer session validation failed:\n"
            + "\n".join(messages)
        )

    return True