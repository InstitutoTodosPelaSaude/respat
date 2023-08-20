def aggregate_results(df, test_id_columns, test_result_columns):
    """
    Aggregates the test results from a single test into a single row.
    Using the specified test_id_columns as the grouping columns.

    The test results in the test_result_columns should be either 'Pos', 'Neg', or 'NT'.

    The final result is 'Pos' if any of the tests was positive.
    'Neg' if none of the tests was positive and at least one was negative.
    'NT' if all of the tests were not performed.

    Args:
        df (pandas DataFrame): dataframe to be fixed
        test_id_columns (list of str): list of columns to be used as grouping columns
        test_result_columns (list of str): list of columns to be aggregated

    Returns:
        pandas DataFrame: dataframe with aggregated results
    """

    df_test_results = df[test_id_columns + test_result_columns].copy()

    df_test_results = (
        df_test_results
        # Mapping the test results to 1, 0, -1
        # and using the max to aggregate to improve performance
        .assign(
            **{
                test_result_column: df[test_result_column].replace(
                    {"Pos": 1, "Neg": 0, "NT": -1}
                )
                for test_result_column in test_result_columns
            }
        )
    )

    df_test_results = (
        df_test_results
        .groupby(test_id_columns)
        .agg(
            # at_least_one_positive
            "max"
        )
    )

    # Join the aggregated test results back with the original dataframe
    df = df.drop(columns=test_result_columns).merge(
        df_test_results, on=test_id_columns, how="inner"
    )

    # map back the test results
    df = df.assign(
        **{
            test_result_column: df[test_result_column].replace(
                {1: "Pos", 0: "Neg", -1: "NT"}
            )
            for test_result_column in test_result_columns
        }
    )

    df = df.reset_index(drop=True)

    return df