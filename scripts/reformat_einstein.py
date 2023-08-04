# -*- coding: utf-8 -*-

# Created by: Anderson Brito
# Email: anderson.brito@itps.org.br
# Release date: 2023-06-19
## Last update: 2023-07-04

import pandas as pd
import os
import numpy as np
import hashlib
import time
import argparse
from epiweeks import Week

import warnings
import logging

from collections import defaultdict

warnings.simplefilter(action="ignore", category=FutureWarning)
warnings.simplefilter(action="ignore", category=pd.errors.PerformanceWarning)

pd.set_option("display.max_columns", 500)
pd.options.mode.chained_assignment = None

today = time.strftime("%Y-%m-%d", time.gmtime())


def generate_id(value):
    """Returns a hash for a given value

    Args:
        value (str): value to be hashed

    Returns:
        str: sha1 hash hexdigest
    """
    return hashlib.sha1(str(value).encode("utf-8")).hexdigest()


def load_table(file):
    """
    Load data from file automatic respecting the file format.
    Compatible file formats: TSV, CSV, XLS, XLSX, PARQUET

    Args:
        file (str): File path

    Returns:
        pandas Dataframe or str: Dataframe or empty string if file format is not compatible.
    """

    df = ""
    if str(file).split(".")[-1] == "tsv":
        separator = "\t"
        df = pd.read_csv(file, encoding="utf-8", sep=separator, dtype="str")
    elif str(file).split(".")[-1] == "csv":
        separator = ","
        df = pd.read_csv(file, encoding="utf-8", sep=separator, dtype="str")
    elif str(file).split(".")[-1] in ["xls", "xlsx"]:
        df_dict = pd.read_excel(file, index_col=None, header=0, sheet_name=None, dtype='str')
        
        df = pd.DataFrame()
        for sheet in df_dict.keys():
            if sheet.startswith("EXAMES"):
                continue

            df = df.append(
                df_dict[sheet]
                .assign(ExcelSheet=sheet)
            )

        return df
    elif str(file).split(".")[-1] == "parquet":
        df = pd.read_parquet(file, engine="auto")
        df.fillna("", inplace=True)
    else:
        print(
            "Wrong file format. Compatible file formats: TSV, CSV, XLS, XLSX, PARQUET"
        )
        exit()

    return df


def fix_datatable(df):
    PATHOGENS = {
        "FLUA": ["Influenza A"],
        "FLUB": ["Influenza B"],
        "VSR": ["Vírus Sincicial Respiratório"],
        "SC2": ["COVID"],
        "META": [],
        "PARA": [],
        "ADENO": [],
        "COVS": [],
        "RINO": [],
        "ENTERO": [],
        "BOCA": [],
        "BAC": [],
    }

    # generate sample id
    df.insert(1, "sample_id", "")
    df.fillna("", inplace=True)

    # dfN = dfL
    dfN = (
        pd.DataFrame()
    )  # create empty dataframe, and populate it with reformatted data from original lab dataframe

    #
    # [WIP] Retrocompatibilidade com dados anteriores
    #
    # date_cols = ["DH_COLETA", "DT_COLETA"]
    # date = "DT_LIBERACAO"
    # for col in date_cols:
    #     if col in df.columns.tolist():
    #         date = col

    # main_id_column = "NU_ACCESSION"
    # if "NU_ACCESSION" not in df.columns.tolist():
    #     main_id_column = "ACCESSION"

    id_columns = ["ACCESSION", "EXAME", "DETALHE_EXAME", "IDADE", "SEXO", "DH_COLETA", "MUNICÍPIO", "ESTADO"]

    for column in id_columns:
        if column not in df.columns.tolist():
            df[column] = ""
            print(
                "\t\t\t - No '%s' column found. Please check for inconsistencies. Meanwhile, an empty '%s' column was added."
                % (column, column)
            )

    # assign id and deduplicate
    df, dfN = deduplicate(df, dfN, id_columns)

    if df.empty:
        return dfN

    df_pivot = (
        df
        .query("PATOGENO not in ('DENGUE', 'VARIOLA SIMIA')")
        .assign(
            DETALHE_EXAME=lambda df: df["DETALHE_EXAME"].fillna("S/ DETALHE"),
        )
        .assign(
            RESULTADO=lambda df: df["RESULTADO"]
            .str.lower()
            .str.strip()
            .map(
                {
                    "não detectado": "Neg",
                    "detectado": "Pos",
                }
            )
        )
        .assign(
            # DIVIDINDO INFLUENZA EM A E B
            PATOGENO=lambda df: df.apply(
                lambda row: "INFLUENZA B"
                if (" B" in row["DETALHE_EXAME"] and "INFLU" in row["DETALHE_EXAME"])
                else "INFLUENZA A"
                if "INF A" in row["DETALHE_EXAME"] or "INFLUENZA" in row["DETALHE_EXAME"]
                else row["PATOGENO"],
                axis=1,
            )
        )
        .pivot_table(
            index=id_columns+["sample_id"],
            columns="PATOGENO",
            values="RESULTADO",
            aggfunc="first",
            fill_value="NT"
        )
        .reset_index()
    )


    # CREATE test_kit COLUMN
    test_kit_dict = {
        "PCR PAINEL DE PATOGENOS RESPIRATORIO":"painel_4",

        "PAINEL MOLECULAR PARA PNEUMONIA":"painel_3",
        "PCR MULTIPLEX ZIKA, DENGUE E CHIKUNG":"painel_3",
        "PCR PARA INFLUENZA A/B E VRS":"painel_3",
        "ZZPAINEL MOLECULAR PARA PNEUMONIA":"painel_3",

        "PESQUISA RÁPIDA PARA INFLUENZA A E B":"painel_2",
        "TESTE RÁPIDO PARA DENGUE IGM E NS1":"antibody",
        "SOROLOGIA PARA DENGUE":"antibody",


        'EXCLUSIVO EMPRESAS PCR COVID-19': 'covid_pcr',
        'OPERAÇÃO AEROPORTO ANTÍGENO COVID-19': 'covid_antigen',
        'OPERAÇÃO AEROPORTO PCR COVID-19': 'covid_pcr',
        'PCR COVID19 EXPRESS': 'covid_pcr',
        'PCR EM TEMPO REAL PARA DETECÇÃO DE C': 'covid_pcr',
        'TESTE MOLECULAR COVID-19, AMPLIFICAÇ': 'covid_pcr',
        'TESTE MOLECULAR COVID-19, AMPLIFICAÇÃO I': 'covid_pcr',
        'TESTE RÁPIDO-ANTÍGENO COVID-19 (SARS': 'covid_antigen',
        'TESTE RÁPIDO-ANTÍGENO COVID-19 (SARS COV': 'covid_antigen',
        'TX PCR COVID19': 'covid_pcr',
        'ZZOPERAÇÃO AEROPORTO ANTÍGENO COVID-19': 'covid_antigen',
        'ZZOPERAÇÃO AEROPORTO PCR COVID-19': 'covid_pcr',
        'ZZPCR PAINEL DE PATOGENOS RESPIRATORIO': 'covid_pcr',
        'ZZTESTE MOLECULAR COVID-19, AMPLIFICAÇ': 'covid_pcr',
        'ZZTESTE RÁPIDO-ANTÍGENO COVID-19 (SARS': 'covid_antigen',
        'HMSC - TESTE MOLECULAR ISOTÉRMICO COVID-': 'covid_pcr',
    }
    # Otherways, assign test_1
    test_kit_dict = defaultdict(lambda: "painel_1", test_kit_dict)

    df_pivot = (
        df_pivot
        .assign(
            test_kit=lambda df: 
            df["EXAME"].map(test_kit_dict),
        )
        .drop(columns=["EXAME", "DETALHE_EXAME"])

        # RENAME COLUMNS
        .rename(
            columns = {
                "INFLUENZA A": "FLUA_test_result",
                "INFLUENZA B": "FLUB_test_result",
                "VIRUS SINCICIAL RESPIRATÓRIO": "VSR_test_result",
                "COVID": "SC2_test_result",

                "ACCESSION": "test_id",

                "DH_COLETA": "date_testing",
                "ESTADO":"state",
                "MUNICÍPIO":"location",
                "IDADE": "age",
                "SEXO": "sex"
            }
        )
    )
    
    # ADDING MISSING COLUMNS
    # df["birthdate"] = ""
    df_pivot["Ct_FluA"] = ""
    df_pivot["Ct_FluB"] = ""
    df_pivot["Ct_VSR"] = ""
    df_pivot["Ct_RDRP"] = ""
    df_pivot["Ct_geneE"] = ""
    df_pivot["Ct_ORF1ab"] = ""
    df_pivot["Ct_geneN"] = ""
    df_pivot["Ct_geneS"] = ""
    df_pivot["geneS_detection"] = ""

    # adding missing pathogen columns
    for pathogen in PATHOGENS.keys():
        if pathogen+"_test_result" not in df_pivot.columns.tolist():
            df_pivot[pathogen+"_test_result"] = "NT"

    print(df_pivot.head())

    dfN = dfN.append(df_pivot, ignore_index=True)

    return dfN


def aggregate_results(df, test_id_columns, test_result_columns, other_agg_rules={}, return_aggregated_df=False):
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
        other_agg_rules (dict): dictionary of other aggregation rules in the pandas format
        return_aggregated_df (bool): whether to return the aggregated dataframe or to join the results back to the original dataframe

    Returns:
        pandas DataFrame: dataframe with aggregated results
    """

    df_test_results = (
        df
        [test_id_columns + test_result_columns + list(other_agg_rules.keys())]
        .copy()
        .groupby(test_id_columns)
        .agg(
            {
                **{
                    test_result_column: lambda x: (
                        'Pos' if 'Pos' in x.values 
                        else 'Neg' if 'Neg' in x.values 
                        else 'NT'
                    )
                    for test_result_column in test_result_columns
                },
                **other_agg_rules
            }
        )
        .reset_index()
    )

    if return_aggregated_df:
        return df_test_results

    # Join the aggregated test results back with the original dataframe
    df = (
        df
        .drop(columns=test_result_columns)
        .drop(columns=list(other_agg_rules.keys()))
        .merge(df_test_results, on=test_id_columns, how='inner')
    ) 

    return df


if __name__ == "__main__":
    FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    formatter = logging.Formatter(FORMAT)

    logger = logging.getLogger("EINSTEIN ETL")
    # add handler to stdout
    handler = logging.StreamHandler()
    # Logger all levels
    handler.setLevel(logging.DEBUG)
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)


    parser = argparse.ArgumentParser(
        description="Performs diverse data processing tasks for specific HIAE cases. It seamlessly loads and combines data from multiple sources and formats into a unified dataframe. It applies renaming and correction rules to columns, generates unique identifiers, and eliminates duplicates based on prior data processing. Age information is derived from birth dates, and sex information is adjusted accordingly. The resulting dataframe is sorted by date and saved as a TSV file. Duplicate rows are also identified and saved separately for further analysis.",
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

    path = os.path.abspath(os.getcwd())
    input_folder = path + "/" + args.datadir + "/"
    rename_file = args.rename
    correction_file = args.correction
    cache_file = args.cache
    output = args.output

    logger.info(f"Starting EINSTEIN ETL")
    logger.info(f"Input folder: {input_folder}")
    logger.info(f"Rename file: {rename_file}")
    logger.info(f"Correction file: {correction_file}")
    logger.info(f"Cache file: {cache_file}")
    logger.info(f"Output file: {output}")

    # load cache file
    if cache_file not in [np.nan, "", None]:
        dfT = load_table(cache_file)
        dfT.fillna("", inplace=True)

    else:
        # dfP = pd.DataFrame()
        dfT = pd.DataFrame()

    # load renaming patterns
    dfR = load_table(rename_file)
    dfR.fillna("", inplace=True)

    dict_rename = {}
    # dict_corrections = {}
    for idx, row in dfR.iterrows():
        id = dfR.loc[idx, "lab_id"]
        if id not in dict_rename:
            dict_rename[id] = {}
        old_colname = dfR.loc[idx, "column_name"]
        new_colname = dfR.loc[idx, "new_name"]
        rename_entry = {old_colname: new_colname}
        dict_rename[id].update(rename_entry)

    # load value corrections
    dfC = load_table(correction_file)
    dfC.fillna("", inplace=True)
    dfC = dfC[
        dfC["lab_id"].isin(["EINSTEIN", "any"])
    ]  ## filter to correct data into fix_values FLEURY

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

    def rename_columns(id, df):
        if id in dict_rename:
            df = df.rename(columns=dict_rename[id])
        return df

    # open data files
    for sub_folder in os.listdir(input_folder):
        if sub_folder == "EINSTEIN":  # check if folder is the correct one
            id = sub_folder
            sub_folder = sub_folder + "/"

            if not os.path.isdir(input_folder + sub_folder):
                logger.error(f"Folder {input_folder + sub_folder} not found.")
                break
            
            logger.info(f"Processing DataFrame from: {id}")

            for filename in sorted(os.listdir(input_folder + sub_folder)):

                if not filename.endswith((".csv", ".tsv", ".xls", ".xlsx")):
                    continue
                if filename.startswith( ('~', '_') ):
                    continue

                logger.info(f"Loading data from: {input_folder + sub_folder + filename}")

                df = load_table(input_folder + sub_folder + filename)
                # df.fillna("", inplace=True)
                df.reset_index(drop=True)

                logger.info(f"Loaded {df.shape[0]} rows and {df.shape[1]} columns")

                # Remove duplicates
                df = df.drop_duplicates(
                    subset=["ACCESSION", "EXAME", 'DETALHE_EXAME'], 
                    keep="last"
                )

                logger.info(f"Removed duplicates. New shape: {df.shape[0]} rows and {df.shape[1]} columns")

                logger.info(f"Starting to fix DataFrame - {filename}")
                df = fix_datatable(df)
                logger.info(f"Finished fixing DataFrame - {filename}")
                logger.info(f"New shape: {df.shape[0]} rows and {df.shape[1]} columns")

                if df.empty:
                    logger.warning(f"Empty DataFrame after fixing - {filename}. Check for inconsistencies.")
                    continue

                logger.info(f"Starting to aggregate results - {filename}")
                df = aggregate_results(
                    df, 
                    [
                        'test_id', 'test_kit'
                    ], 
                    [
                        'FLUB_test_result',
                        'FLUA_test_result',
                        'VSR_test_result',
                        'SC2_test_result',
                        'META_test_result',
                        'RINO_test_result',
                        'PARA_test_result',
                        'ADENO_test_result',
                        'BOCA_test_result',
                        'COVS_test_result',
                        'ENTERO_test_result',
                        'BAC_test_result',
                    ],
                    {
                        col: 'max'
                        for col in ['date_testing', 'age', 'sex', 'location', 'state', 'sample_id']
                    },
                    return_aggregated_df=True
                )
                logger.info(f"Finished aggregating results - {filename}")
                logger.info(f"New shape: {df.shape[0]} rows and {df.shape[1]} columns")

                if df.empty:
                    logger.warning(f"Empty DataFrame after fixing - {filename}. Check for inconsistencies.")
                    continue

                df.insert(0, "lab_id", id)
                # df = rename_columns(id, df)  # fix data points

                dfT = dfT.reset_index(drop=True)
                df = df.reset_index(drop=True)

                frames = [dfT, df]
                df2 = pd.concat(frames).reset_index(drop=True)
                dfT = df2

                logger.info(f"Finished processing file: {filename}")

    dfT = dfT.reset_index(drop=True)
    dfT.fillna("", inplace=True)

    # [WIP] Update fix data points
    # fix data points
    # def fix_data_points(id, col_name, value):
    #     new_value = value
    #     if value in dict_corrections[id][col_name]:
    #         new_value = dict_corrections[id][col_name][value]
    #     return new_value

    # # print(dfT.head())
    # print("\n# Fixing data points...")
    # for lab_id, columns in dict_corrections.items():
    #     print("\t- Fixing data from: " + lab_id)
    #     for column, values in columns.items():
    #         # print('\t- ' + column + ' (' + column + ' → ' + str(values) + ')')
    #         dfT[column] = dfT[column].apply(
    #             lambda x: fix_data_points(lab_id, column, x)
    #         )

    # reformat dates and convert to datetime format
    dfT["date_testing"] = pd.to_datetime(
        dfT["date_testing"], dayfirst=True
    )  # , format='%Y-%m-%d', errors='ignore'

    # create epiweek column
    def get_epiweeks(date):
        try:
            date = pd.to_datetime(date)
            epiweek = str(Week.fromdate(date, system="cdc"))  # get epiweeks
            year, week = epiweek[:4], epiweek[-2:]
            epiweek = str(Week(int(year), int(week)).enddate())
        except:
            epiweek = ""
        return epiweek

    dfT["epiweek"] = dfT["date_testing"].apply(lambda x: get_epiweeks(x))

    # [WIP] Handle Birthdate
    # add age from birthdate, if age is missing
    # if "birthdate" in dfT.columns.tolist():
    #     for idx, row in dfT.iterrows():
    #         birth = dfT.loc[idx, "birthdate"]
    #         test = dfT.loc[idx, "date_testing"]
    #         if birth not in [np.nan, "", None]:
    #             birth = pd.to_datetime(birth)
    #             age = (test - birth) / np.timedelta64(1, "Y")
    #             dfT.loc[idx, "age"] = np.round(age, 1)

    # fix sex information
    dfT["sex"] = dfT["sex"].apply(lambda x: x.upper()[0] if x != "" else x)

    # reset index
    dfT = dfT.reset_index(drop=True)
    # dfT.to_csv(output, sep='\t', index=False)

    key_cols = [
        "lab_id",
        "test_id",
        "test_kit",
        "patient_id",
        "sample_id",
        "state",
        "location",
        "date_testing",
        "epiweek",
        "age",
        "sex",
        "FLUA_test_result",
        "Ct_FluA",
        "FLUB_test_result",
        "Ct_FluB",
        "VSR_test_result",
        "Ct_VSR",
        "SC2_test_result",
        "Ct_geneE",
        "Ct_geneN",
        "Ct_geneS",
        "Ct_ORF1ab",
        "Ct_RDRP",
        "geneS_detection",
        "META_test_result",
        "RINO_test_result",
        "PARA_test_result",
        "ADENO_test_result",
        "BOCA_test_result",
        "COVS_test_result",
        "ENTERO_test_result",
        "BAC_test_result",
    ]

    for col in dfT.columns.tolist():
        if col not in key_cols:
            dfT = dfT.drop(columns=[col])

    for col in key_cols:
        if col not in dfT.columns.tolist():
            logger.warning(f"Column {col} not found in the table. Adding it with empty values.")
            dfT[col] = ''

    # keep only key columns, and find null dates
    dfT = dfT[key_cols]

    def date2str(value):
        try:
            value = value.strftime("%Y-%m-%d")
        except:
            value = ""
        return value

    dfT["date_testing"] = dfT["date_testing"].apply(lambda x: date2str(x))

    # output duplicates rows
    duplicates = dfT.duplicated().sum()
    if duplicates > 0:
        mask = dfT.duplicated(keep=False)  # find duplicates
        dfD = dfT[mask]
        output2 = input_folder + "duplicates.tsv"
        dfD.to_csv(output2, sep="\t", index=False)

        logger.warning(f"Found {duplicates} duplicate entries in the table. Please check the file {output2}.")

    # drop duplicates
    dfT = dfT.drop_duplicates(keep="last")

    # sorting by date
    dfT = dfT.sort_values(by=["lab_id", "test_id", "date_testing"])

    # output combined dataframe
    dfT.to_csv(output, sep="\t", index=False)
    logger.info(f"Data successfully aggregated and saved in: {output}")

# python scripts/reformat_einstein.py --datadir data --rename data/rename_columns.xlsx --correction data/fix_values.xlsx --output combined_test_einstein.tsv