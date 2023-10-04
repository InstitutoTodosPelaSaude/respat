import logging
import os
from logging import getLogger, INFO, StreamHandler, Formatter

class LoggerSingleton:
    loggers = {}
    FORMAT = "%(asctime)s - %(name)-30s - %(levelname)s - %(message)s"

    def __init__(self):
        pass

    @staticmethod
    def get_logger(name: str):
        if name not in LoggerSingleton.loggers:
            # create a stdout logger
            LoggerSingleton.loggers[name] = getLogger(name)
            LoggerSingleton.loggers[name].setLevel("INFO")
            handler = StreamHandler()
            handler.setLevel(INFO)
            formatter = Formatter(LoggerSingleton.FORMAT)
            handler.setFormatter(formatter)
            LoggerSingleton.loggers[name].addHandler(handler)

        return LoggerSingleton.loggers[name]

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
    logger = LoggerSingleton().get_logger("AGGREGATE RESULTS")

    df_test_results = df[test_id_columns + test_result_columns].copy()

    logger.info(f"Starting aggregation of {len(df_test_results)} rows")
    logger.info(f"Using {test_id_columns} as grouping columns")
    logger.info(f"Using {test_result_columns} as test result columns")

    # Replace the test results with 1, 0, -1
    logger.info("Mapping test results to 1, 0, -1")

    # SHOW WARNING IF THERE ARE ANY OTHER VALUES THAN Pos, Neg, NT
    for test_result_column in test_result_columns:
        test_result_column_values = set(df_test_results[test_result_column].unique())
        if len(test_result_column_values - {"Pos", "Neg", "NT"}) > 0:
            logger.warning(
                f"Column {test_result_column} contains values other than Pos, Neg, NT - {test_result_column_values}"
            )

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

    logger.info("Aggregating test results using AT LEAST ONE POSITIVE")

    df_test_results = (
        df_test_results
        .groupby(test_id_columns)
        .agg(
            # at_least_one_positive
            "max"
        )
    )

    logger.info("Joining aggregated test results back to the original dataframe")

    # Join the aggregated test results back with the original dataframe
    df = df.drop(columns=test_result_columns).merge(
        df_test_results, on=test_id_columns, how="inner"
    )

    print(df.columns)

    logger.info("Mapping test results back to Pos, Neg, NT")

    print(df.columns)

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


def has_something_to_be_done( data_folder ):

    for dir in os.listdir(data_folder):
        if not dir.endswith( ('.tsv', '.csv', '.xls', '.xlsx', '.parquet') ):
            continue
        if dir.startswith( ('~', '_') ):
            continue
        
        return True

    return False
