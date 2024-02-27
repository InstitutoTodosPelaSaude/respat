import pandas as pd
import os
import numpy as np
import hashlib
import time
import argparse
from epiweeks import Week

from utils import aggregate_results, has_something_to_be_done

import logging

import warnings

warnings.simplefilter(action="ignore", category=FutureWarning)
warnings.simplefilter(action="ignore", category=pd.errors.PerformanceWarning)

pd.set_option("display.max_columns", 500)
pd.options.mode.chained_assignment = None

today = time.strftime("%Y-%m-%d", time.gmtime())

def load_table(file, separator=None):
    """Load table from file.

    Args:
        file (str): File path.
        separator (str, optional): Separator. Defaults to None.

    Returns:
        pd.DataFrame: Table.
    """
    df = ""
    if str(file).split(".")[-1] == "tsv":
        separator = "\t" if separator is None else separator
        df = pd.read_csv(file, encoding="latin-1", sep=separator, dtype="str")
    elif str(file).split(".")[-1] == "csv":
        separator = "," if separator is None else separator
        df = pd.read_csv(file, encoding="latin-1", sep=separator, dtype="str")
    elif str(file).split(".")[-1] in ["xls", "xlsx"]:
        df = pd.read_excel(file, index_col=None, header=0, sheet_name=0, dtype="str")
        df.fillna("", inplace=True)
    else:
        print("Wrong file format. Compatible file formats: TSV, CSV, XLS, XLSX")
        exit()
    return df

def get_epiweeks(date):
    """Get epiweeks from date.

    Args:
        date (str): Date in format YYYY-MM-DD.

    Returns:
        str: Epiweek in format YYYY-MM-DD.
    """
    try:
        date = pd.to_datetime(date)
        epiweek = str(Week.fromdate(date, system="cdc"))  # get epiweeks
        year, week = epiweek[:4], epiweek[-2:]
        epiweek = str(Week(int(year), int(week)).enddate())
    except:
        epiweek = ""
    return epiweek

def fix_datatable(df):
    """Fix datatable.

    Args:
        df (pd.DataFrame): Dataframe.

    Returns:
        pd.DataFrame: Fixed dataframe.
    """

    return df

if __name__ == "__main__":
    # Config logger
    FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    logger = logging.getLogger("DB DIAGNÓSTICOS ETL")
    handler = logging.StreamHandler() # Add handler to stdout
    handler.setLevel(logging.DEBUG) # Logger all levels
    formatter = logging.Formatter(FORMAT)
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)

    logger.info(f"Starting DB DIAGNÓSTICOS ETL")

    # Parse arguments
    parser = argparse.ArgumentParser(
        description="Performs diverse data processing tasks for specific Fleury lab cases. It seamlessly loads and combines data from multiple sources and formats into a unified dataframe. It applies renaming and correction rules to columns, generates unique identifiers, and eliminates duplicates based on prior data processing. Age information is derived from birth dates, and sex information is adjusted accordingly. The resulting dataframe is sorted by date and saved as a TSV file. Duplicate rows are also identified and saved separately for further analysis.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--datadir",
        required=True,
        help="Name of the folder containing independent folders for each lab",
    )
    parser.add_argument(
        "--rename",
        required=False,
        help="TSV, CSV, or excel file containing new standards for column names",
    )
    parser.add_argument(
        "--correction",
        required=False,
        help="TSV, CSV, or excel file containing data points requiring corrections",
    )
    parser.add_argument(
        "--cache", required=False, help="Previously processed data files"
    )
    parser.add_argument(
        "--output",
        required=True,
        help="TSV file aggregating all columns listed in the 'rename file'",
    )
    args = parser.parse_args()

    # Get arguments
    path = os.path.abspath(os.getcwd())
    input_folder = path + "/" + args.datadir + "/"
    rename_file = args.rename
    correction_file = args.correction
    cache_file = args.cache
    output = args.output
    lab_data_folder = input_folder + 'DB Diag/'

    # Log arguments
    logger.info(f"Input folder: {input_folder}")
    logger.info(f"Rename file: {rename_file}")
    logger.info(f"Correction file: {correction_file}")
    logger.info(f"Cache file: {cache_file}")
    logger.info(f"Output file: {output}")

    # Check if there is files to be processed
    if not has_something_to_be_done(lab_data_folder):
        print(f"No files found in {lab_data_folder}")
        if cache_file not in [np.nan, '', None]: 
            print(f"Just copying {cache_file} to {output}")
            os.system(f"cp {cache_file} {output}")
        else:
            print(f"No cache file found. Nothing to be done.")
        print(f"Data successfully aggregated and saved in: {output}")
        exit()

    # Load cache file
    if cache_file not in [np.nan, "", None]:
        logger.info(f"Loading cache file: {cache_file}")

        dfT = load_table(cache_file)
        dfT.fillna("", inplace=True)

        # fix state and location encoding
        dfT["state"] = dfT["state"].apply(lambda x: x.encode("latin-1").decode("utf-8"))
        dfT["location"] = dfT["location"].apply(
            lambda x: x.encode("latin-1").decode("utf-8")
        )
    else:
        logger.info(f"No cache file provided. Starting from scratch.")
        dfT = pd.DataFrame()

    # Load renaming patterns
    dfR = load_table(rename_file)
    dfR.fillna("", inplace=True)

    # Transform rename correction patterns in a dictionary
    dict_rename = {}
    for idx, row in dfR.iterrows():
        id = dfR.loc[idx, "lab_id"]
        if id not in dict_rename:
            dict_rename[id] = {}
        old_colname = dfR.loc[idx, "column_name"]
        new_colname = dfR.loc[idx, "new_name"]
        rename_entry = {old_colname: new_colname}
        dict_rename[id].update(rename_entry)

    # Load value correction patterns
    dfC = load_table(correction_file)
    dfC.fillna("", inplace=True)
    dfC = dfC[
        dfC["lab_id"].isin(["DB Diag", "any"])
    ] 

    # Transform value correction patterns in a dictionary
    dict_corrections = {}
    all_ids = list(set(dfC["lab_id"].tolist()))
    for idx, row in dfC.iterrows():
        lab_id = dfC.loc[idx, "lab_id"]
        colname = dfC.loc[idx, "column_name"]

        old_data = dfC.loc[idx, "old_data"]
        new_data = dfC.loc[idx, "new_data"]
        if old_data + new_data not in [""]:
            labs = []
            if colname == "any":
                labs = all_ids
            else:
                labs = [lab_id]
            for id in labs:
                if id not in dict_corrections:
                    dict_corrections[id] = {}
                if colname not in dict_corrections[id]:
                    dict_corrections[id][colname] = {}
                data_entry = {old_data: new_data}
                dict_corrections[id][colname].update(data_entry)

    # Define some functions to be used in the pipeline
    def generate_id(column_id):
        id = hashlib.sha1(str(column_id).encode("utf-8")).hexdigest()
        return id

    def deduplicate(dfL, dfN, id_columns):
        # generate sample id
        dfL["unique_id"] = (
            dfL[id_columns].astype(str).sum(axis=1)
        )  # combine values in rows as a long string
        dfL["sample_id"] = dfL["unique_id"].apply(
            lambda x: generate_id(x)[:16]
        )  # generate alphanumeric sample id

        # prevent reprocessing of previously processed samples
        if cache_file not in [np.nan, "", None]:
            duplicates = set(
                dfL[dfL["sample_id"].isin(dfT["sample_id"].tolist())][
                    "sample_id"
                ].tolist()
            )
            if len(duplicates) == len(set(dfL["sample_id"].tolist())):
                print(
                    "\n\t\t * ALL samples (%s) were already previously processed. All set!"
                    % len(duplicates)
                )
                dfN = (
                    pd.DataFrame()
                )  # create empty dataframe, and populate it with reformatted data from original lab dataframe
                dfL = pd.DataFrame()
                return dfN, dfL
            else:
                print(
                    "\n\t\t * A total of %s out of %s samples were already previously processed."
                    % (str(len(duplicates)), str(len(set(dfL["sample_id"].tolist()))))
                )
                new_samples = len(set(dfL["sample_id"].tolist())) - len(duplicates)
                print("\t\t\t - Processing %s new samples..." % (str(new_samples)))
                dfL = dfL[
                    ~dfL["sample_id"].isin(dfT["sample_id"].tolist())
                ]  # remove duplicates
        else:
            new_samples = len(dfL["sample_id"].tolist())
            print("\n\t\t\t - Processing %s new samples..." % (str(new_samples)))
        return dfL, dfN

    # Open each file and process it
    for sub_folder in os.listdir(input_folder):
        if sub_folder == "DB Diag":
            id = sub_folder
            sub_folder = sub_folder + "/"

            if not os.path.isdir(input_folder + sub_folder):
                logger.error(f"Folder {input_folder + sub_folder} not found.")
                break

            logger.info(f"Processing DataFrame from: {id}")

            for file_i, filename in enumerate(sorted(os.listdir(input_folder + sub_folder))):
                if not filename.endswith((".tsv", ".csv", ".xls", ".xlsx", ".parquet")):
                    continue
                if filename.startswith(("~", "_")):
                    continue

                logger.info(
                    f"Loading data from: {input_folder + sub_folder + filename}"
                )
                logger.info(f"File {file_i + 1} of {len(os.listdir(input_folder + sub_folder))}")
                df = load_table(input_folder + sub_folder + filename, separator="\t")

                logger.info(f"Loaded {df.shape[0]} rows and {df.shape[1]} columns")

                logger.info(f"Starting to fix DataFrame - {filename}")
                df = fix_datatable(df)
                logger.info(f"Finished fixing DataFrame - {filename}")
                logger.info(f"New shape: {df.shape[0]} rows and {df.shape[1]} columns")

                if df.empty:
                    logger.warning(
                        f"Empty DataFrame after fixing - {filename}. Check for inconsistencies."
                    )
                    continue

                df.insert(0, "lab_id", id)
                if id in dict_rename:
                    df = df.rename(columns=dict_rename[id])
                else:
                    logger.warning(
                        f"No renaming rules found for {id}. Check for inconsistencies."
                    )


                logger.info(f"Starting to fix values - {filename}")

                # Joining the generic corrections with the lab-specific ones
                dict_corrections_full = {
                    #**dict_corrections['FLEURY'] 
                    **dict_corrections['any']
                }
                df = df.replace(dict_corrections_full)

                logger.info(f"Finished fixing values - {filename}")
                logger.info(f"New shape: {df.shape[0]} rows and {df.shape[1]} columns")
                logger.info(f"Starting to aggregate results - {filename}")

                df = aggregate_results(
                    df,
                    ["test_id", "test_kit"],
                    [
                        "FLUB_test_result",
                        "FLUA_test_result",
                        "VSR_test_result",
                        "SC2_test_result",
                        "META_test_result",
                        "RINO_test_result",
                        "PARA_test_result",
                        "ADENO_test_result",
                        "BOCA_test_result",
                        "COVS_test_result",
                        "ENTERO_test_result",
                        "BAC_test_result",
                    ],
                )

                logger.info(f"Finished aggregating results - {filename}")
                logger.info(f"New shape: {df.shape[0]} rows and {df.shape[1]} columns")

                dfT = dfT.reset_index(drop=True)
                df = df.reset_index(drop=True)

                frames = [dfT, df]
                df2 = pd.concat(frames).reset_index(drop=True)
                dfT = df2

                logger.info(f"Finished processing file: {filename}")