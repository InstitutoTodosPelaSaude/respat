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

from utils import aggregate_results, has_something_to_be_done

import warnings
import logging

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
        
        dfs = []
        for sheet in df_dict.keys():
            if sheet.startswith("EXAMES"):
                continue

            dfs.append(
                df_dict[sheet]
                .assign(ExcelSheet=sheet)
            )

        return pd.concat(dfs)
    
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
    
    df = df.query("PATOGENO not in ('DENGUE', 'VARIOLA SIMIA')")

    # generate sample id
    df.insert(1, "sample_id", "")
    df.fillna("", inplace=True)

    id_columns = [
        "ACCESSION", 
        "EXAME",
        # "MUNICÍPIO",
        # "EXAME",
        # "IDADE",
        # "SEXO", 
        # "DH_COLETA",
        # "RESULTADO"
    ]

    for column in id_columns:
        if column not in df.columns.tolist():
            df[column] = ""
            print(
                "\t\t\t - No '%s' column found. Please check for inconsistencies. Meanwhile, an empty '%s' column was added."
                % (column, column)
            )

    # assign id and deduplicate
    dfN = df
    df, dfN = deduplicate(df, dfN, id_columns)
    if df.empty:
        return dfN
    
    df['ACCESSION'] = df['ACCESSION'].astype(int)
    # Fix age column
    df_fix_age_column = (
        df
        [ id_columns + ['IDADE'] ]
        .assign(
            IDADE=df['IDADE'].astype(int)
        )
        .groupby(id_columns)
        .max()
    )

    # replace the fixed age column on the original dataframe
    df = (
        df
        .drop(columns=['IDADE'], axis=1)
        .merge(
            df_fix_age_column, 
            on=id_columns, 
            how='inner'
        )
    )
    df['IDADE'] = df['IDADE'].fillna(-1).astype(int)

    df_pivot = (
        df.assign(
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
            # CORRIGINDO PATOGENO INFLUENZA
            PATOGENO=lambda df: df.apply(
                lambda row: "INFLUENZA B"
                if "INFLUENZA B" in row["DETALHE_EXAME"] or "INFLUENZA  B" in row["DETALHE_EXAME"]
                else "INFLUENZA A"
                if "INF A" in row["DETALHE_EXAME"] or "INFLUENZA" in row["DETALHE_EXAME"]
                else row["PATOGENO"],
                axis=1,
            )
        )
    )

    # Assign results to each pathogen
    PATHOGEN_NAMES = {
        'SC2': {'COVID'},
        'FLUA': {'INFLUENZA A'},
        'FLUB': {'INFLUENZA B'},
        'VSR': {'VIRUS SINCICIAL RESPIRATÓRIO'},
        'META':{},
        'RINO':{},
        'PARA':{},
        'ADENO':{},
        'BOCA':{},
        'COVS':{},
        'BAC':{},
        'ENTERO':{},
    }

    for pathogen, name_list in PATHOGEN_NAMES.items():
        test_result = pathogen + '_test_result'
        df_pivot[test_result] = df_pivot.apply(
            lambda x: 'NT' if x['PATOGENO'] not in name_list else x['RESULTADO'], 
            axis=1
        )

    # CREATE test_kit COLUMN
    test_kit_dict = {
        "PCR PAINEL DE PATOGENOS RESPIRATORIO":"test_4",

        "PAINEL MOLECULAR PARA PNEUMONIA":"test_3",
        "PCR MULTIPLEX ZIKA, DENGUE E CHIKUNG":"test_3",
        "PCR PARA INFLUENZA A/B E VRS":"test_3",
        "ZZPAINEL MOLECULAR PARA PNEUMONIA":"test_3",

        "PESQUISA RÁPIDA PARA INFLUENZA A E B":"test_2",


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

        '24 HRS COMPANHIA AÉREA - PCR COVID19': 'covid_pcr',
        'PESQ RÁPIDA VÍRUS SINCICIAL RESPIRAT': 'vsr_antigen',
    }

    df_pivot = (
        df_pivot
        .assign(
            test_kit=lambda df: 
            df["EXAME"].replace(test_kit_dict),
        )
        .drop(columns=["EXAME"], axis=1)
        .drop(columns=["DETALHE_EXAME"], axis=1)
    )

    print("\n\nTest 2")
    print(df_pivot.columns.tolist())
    df_pivot =(
        # RENAME COLUMNS
        df_pivot
        .rename(
            columns = {
                "ACCESSION": "test_id",

                "DH_COLETA": "date_testing",
                "ESTADO":"state",
                "MUNICÍPIO":"location",
                "IDADE": "age",
                "SEXO": "sex"   
            }
        )
    )
    df_pivot['sex'] = df_pivot['sex'].fillna('I')
    
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

    return df_pivot

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

    lab_data_folder = input_folder + 'EINSTEIN/'
    if not has_something_to_be_done(lab_data_folder):
        print(f"No files found in {lab_data_folder}")
        if cache_file not in [np.nan, '', None]: 
            print(f"Just copying {cache_file} to {output}")
            os.system(f"cp {cache_file} {output}")
        else:
            print(f"No cache file found. Nothing to be done.")
        print(f"Data successfully aggregated and saved in: {output}")
        exit()


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

                if df.empty:
                    logger.warning(f"Empty DataFrame after fixing - {filename}. Check for inconsistencies.")
                    continue

                logger.info(f"New shape: {df.shape[0]} rows and {df.shape[1]} columns")
                dict_corrections_full = {
                    #**dict_corrections['EINSTEIN'] 
                    **dict_corrections['any']
                }
                df = df.replace(dict_corrections_full)

                if df.empty:
                    logger.warning(f"Empty DataFrame after fixing - {filename}. Check for inconsistencies.")
                    continue

                logger.info(f"Starting to aggregate results - {filename}")
                df = aggregate_results(
                    df, 
                    [
                        'test_id', 'test_kit', 'sample_id'
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

    # reformat dates and convert to datetime format
    dfT["date_testing"] = pd.to_datetime(
        dfT["date_testing"], dayfirst=True, errors='ignore'
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