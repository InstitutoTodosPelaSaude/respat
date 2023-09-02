# -*- coding: utf-8 -*-

# Created by: Anderson Brito
# Email: anderson.brito@itps.org.br
# Release date: 2022-12-15
## Last update: 2023-09-01
## Refactor by Bragatte e João Pedro

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

def fix_datatable(df):
    # ignore = ['Influenza A e B - teste rápido', 'Virusmol, Rinovírus/Enterovírus', '']
    PATHOGEN_NORMALIZATION_DICT = {
        "FLUA": [
            "Covidflursvgx - Influenza A",
            "Virusmol, Influenza A",
            "Virusmol, Influenza A/H1",
            "Virusmol, Influenza A/H1-2009",
            "Virusmol, Influenza A/H3",
            "Vírus Influenza A (Sazonal)",
            "Vï¿½rus Influenza A (Sazonal)",
            "Vírus respiratórios - Influenzavirus A",

            # Artificially added
            "Influenza A",
        ],
        "FLUB": [
            "Covidflursvgx - Influenza B",
            "Virusmol, Influenza B",
            "Vírus respiratórios - Influenzavirus B",

            # Artificially added
            "Influenza B",
        ],
        "VSR": [
            "Covidflursvgx - Vírus Sincicial Respiratório",
            "Covidflursvgx - Vï¿½rus Sincicial Respiratï¿½rio",
            "Virusmol, Vírus Sincicial Respiratório",
            "Virusmol, Vï¿½rus Sincicial Respiratï¿½rio",
            "Vï¿½rus Sincial Respiratï¿½rio",
            "Vírus Sincial Respiratório",
            "Vírus respiratório - Sincicial",
            "Vï¿½rus respiratï¿½rio - Sincicial",
            "Vírus respiratórios - Vírus Sincicial Respira",
        ],
        "SC2": [
            "Covid 19, Antígeno, teste rápido",
            "Covid 19, Antï¿½geno, teste rï¿½pido",
            "Covid 19, Detecção por PCR",
            "Covid 19, Detecï¿½ï¿½o por PCR",
            "Covidflursvgx - SARS-CoV-2",
            "Virusmol, SARS-CoV-2",
        ],
        "META": [
            "Virusmol, Metapneumovírus Humano",
            "Virusmol, Metapneumovï¿½rus Humano",
        ],
        "PARA": [
            "Virusmol, Parainfluenza 1",
            "Virusmol, Parainfluenza 2",
            "Virusmol, Parainfluenza 3",
            "Virusmol, Parainfluenza 4",
            "Vírus respiratórios - Parainfluenzavirus 1",
            "Vírus respiratórios - Parainfluenzavirus 2",
            "Vírus respiratórios - Parainfluenzavirus 3",
            "Vírus respiratórios - Parainfluenzavirus 4",
        ],
        "ADENO": [
            "Virusmol, Adenovírus",
            "Virusmol, Adenovï¿½rus",
            "Vírus respiratórios - Adenovírus",
        ],
        "COVS": [
            "Virusmol, Coronavírus 229E",
            "Virusmol, Coronavï¿½rus 229E",
            "Virusmol, Coronavírus HKU1",
            "Virusmol, Coronavï¿½rus HKU1",
            "Virusmol, Coronavírus NL63",
            "Virusmol, Coronavï¿½rus NL63",
            "Virusmol, Coronavírus OC43",
            "Virusmol, Coronavï¿½rus OC43",
        ],
        "RINO": [
            "Virusmol, Rinovï¿½rus/Enterovï¿½rus",
            "Virusmol, Rinovírus/Enterovírus",
        ],
        "ENTERO": [],
        "BOCA": [],
        "BAC": [
            "Virusmol, Bordetella parapertussis",
            "Virusmol, Bordetella pertussis",
            "Virusmol, Chlamydophila pneumoniae",
            "Virusmol, Mycoplasma pneumoniae",
        ],
    }

    df = df.dropna(subset=["CODIGO REQUISICAO", "DATA COLETA", "IDADE"]).reset_index(
        drop=True
    )

    # remove useless lines from panel tests AGRESPVIR
    ignore = {"Vï¿½rus respiratï¿½rios - detecï¿½ï¿½o", "INCONCLUSIVO", ""}
    df = df[~df["PATOGENO"].isin(ignore)]
    df = df[~df["RESULTADO"].isin(ignore)]

    # generate sample id
    df.insert(1, "sample_id", "")
    # df.fillna('', inplace=True)

    # dfN = dfL
    dfN = (
        pd.DataFrame()
    )  # create empty dataframe, and populate it with reformatted data from original lab dataframe
    id_columns = [
        "CODIGO REQUISICAO",
        "PACIENTE",
        "IDADE",
        "SEXO",
        "DATA COLETA",
        "MUNICIPIO",
        "ESTADO",
    ]

    for column in id_columns:
        if column not in df.columns.tolist():
            df[column] = ""
            print(
                "\t\t\t - No '%s' column found. Please check for inconsistencies. Meanwhile, an empty '%s' column was added."
                % (column, column)
            )

    # missing columns
    # adding missing columns
    df["birthdate"] = ""
    df["Ct_FluA"] = ""
    df["Ct_FluB"] = ""
    df["Ct_VSR"] = ""
    df["Ct_RDRP"] = ""
    df["Ct_geneE"] = ""
    df["Ct_ORF1ab"] = ""
    df["Ct_geneN"] = ""
    df["Ct_geneS"] = ""
    df["geneS_detection"] = ""

    # assign id and deduplicate
    df, dfN = deduplicate(df, dfN, id_columns)

    if df.empty:
        return dfN

    # Fixing AGE column
    # Examples: NaN, None, 3A2M, 1M2D

    fix_age_fleury = (
        lambda age: -1
        if age in (None, np.nan)
        else int(age.split("A")[0])
        if "A" in age
        else 0
        if "D" in age
        else int(age)
        if "M" not in age
        else 0
    )

    df = df.assign(IDADE=df["IDADE"].apply(fix_age_fleury))

    # Fixing Gender information
    df["SEXO"] = df["SEXO"].apply(lambda x: x[0] if x != "" else x)

    # Handling Influenza A e B - teste rápido
    # Breaking into two rows
    df = (
        df.assign(
            PATOGENO=df["PATOGENO"].mask(
                df["PATOGENO"].fillna("").str.startswith("Influenza A e B"),
                "Influenza A;Influenza B",
            )
        )
        .assign(PATOGENO=lambda df: df["PATOGENO"].str.split(";"))
        .explode("PATOGENO")
    )

    # Fixing result column
    # UPPERCASE
    df["RESULTADO"] = df["RESULTADO"].str.upper()
    df["RESULTADO"] = df["RESULTADO"].fillna("NT")
    df["RESULTADO"] = df["RESULTADO"].replace("P O S I T I V O", "POSITIVO")
    df["RESULTADO"] = df["RESULTADO"].str.strip()

    # Correção dos exames de INFLUENZA A e B
    # INFLUENZA A e B - Positivo, INFLUENZA A - Positivo, INFLUENZA B - Positivo
    df["RESULTADO"] = df.apply(
        lambda row: "POSITIVO"
        if (row["RESULTADO"].startswith("INFLUENZA A E B"))
        or (
            row["PATOGENO"] == "Influenza A"
            and row["RESULTADO"].startswith("INFLUENZA A")
        )
        or (
            row["PATOGENO"] == "Influenza B"
            and row["RESULTADO"].startswith("INFLUENZA B")
        )
        else "NEGATIVO"
        if (
            row["PATOGENO"] == "Influenza A"
            and row["RESULTADO"].startswith("INFLUENZA B")
        )
        or (
            row["PATOGENO"] == "Influenza B"
            and row["RESULTADO"].startswith("INFLUENZA A")
        )

        else row["RESULTADO"],
        axis=1,
    )

    df["RESULTADO"] = df["RESULTADO"].apply(
        lambda x: "Neg" if "NEGATIVO" in x else "Pos" if "POSITIVO" in x else "NT"
    )

    # Creating result column for each pathogen
    for pathogen, parameter_list in PATHOGEN_NORMALIZATION_DICT.items():
        test_result = pathogen + "_test_result"

        df[test_result] = df["RESULTADO"].where(
            df["PATOGENO"].isin(parameter_list), "NT"
        )

    # Creating test_kit column
    EXAMS_COVID_PCR = {
        "2019NCOV",
        "COVID19GX",
        "COVID19SALI",
        "COVID19POCT",
        "2019NCOV",
        "COVID19GX",
        "COVID19SALI",
    }
    EXAMS_COVID_ANTIGEN = {"AGCOVIDNS"}
    EXAMS_VSR_ANTIGEN = {"AGSINCURG", "VRSAG"}
    EXAMS_FLU_ANTIGEN = {"AGINFLU"}
    EXAMS_FLU_PCR = {"INFLUENZAPCR"}
    EXAMS_TEST_4 = {"COVIDFLURSVGX"}
    EXAMS_TEST_21 = {"VIRUSMOL"}

    df["test_kit"] = df["EXAME"].apply(
        lambda x: "flu_antigen"
        if x in EXAMS_FLU_ANTIGEN
        else "vsr_antigen"
        if x in EXAMS_VSR_ANTIGEN
        else "flu_pcr"
        if x in EXAMS_FLU_PCR
        else "test_4"
        if x in EXAMS_TEST_4
        else "test_21"
        if x in EXAMS_TEST_21
        else "covid_pcr"
        if x in EXAMS_COVID_PCR
        else "covid_antigen"
        if x in EXAMS_COVID_ANTIGEN
        else "unknown"
    )

    df = df.drop(columns=["PATOGENO", "EXAME", "RESULTADO"])

    return df


def load_table(file, separator=None):
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


if __name__ == "__main__":
    FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    logger = logging.getLogger("FLEURY ETL")
    # add handler to stdout
    handler = logging.StreamHandler()
    # Logger all levels
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(FORMAT)
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)

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

    path = os.path.abspath(os.getcwd())
    input_folder = path + "/" + args.datadir + "/"
    rename_file = args.rename
    correction_file = args.correction
    cache_file = args.cache
    output = args.output

    logger.info(f"Starting FLEURY ETL")
    logger.info(f"Input folder: {input_folder}")
    logger.info(f"Rename file: {rename_file}")
    logger.info(f"Correction file: {correction_file}")
    logger.info(f"Cache file: {cache_file}")
    logger.info(f"Output file: {output}")

    lab_data_folder = input_folder + 'FLEURY/'
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
        dfC["lab_id"].isin(["FLEURY", "any"])
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

    # Fix datatables
    # open data files
    for sub_folder in os.listdir(input_folder):
        if sub_folder == "FLEURY":
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
                df = df.rename(columns=dict_rename[id])

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

    dfT = dfT.reset_index(drop=True)
    dfT.fillna("", inplace=True)

    # reformat dates and convert to datetime format
    logger.info(
        f"Finished processing all files. Final shape: {dfT.shape[0]} rows and {dfT.shape[1]} columns"
    )

    dfT["date_testing"] = pd.to_datetime(
        dfT["date_testing"] , dayfirst=True
    )  # , format='%Y-%m-%d', errors='ignore'

    dfT["epiweek"] = dfT["date_testing"].apply(lambda x: get_epiweeks(x))

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

    # keep only key columns, and find null dates
    dfT = dfT[key_cols]

    def date2str(value):
        try:
            value = value.strftime("%Y-%m-%d")
        except:
            value = ""
        return value

    dfT["date_testing"] = dfT["date_testing"].apply(lambda x: date2str(x))

    # dfT['date_testing'] = dfT['date_testing'].apply(lambda x: x.strftime('%Y-%m-%d') if x is pd.Timestamp else '')

    # output duplicates rows
    duplicates = dfT.duplicated().sum()
    if duplicates > 0:
        mask = dfT.duplicated(keep=False)  # find duplicates
        dfD = dfT[mask]
        output2 = input_folder + "duplicates.tsv"
        dfD.to_csv(output2, sep="\t", index=False)
        logger.warning(f"File with {duplicates} duplicate entries saved in: {output2}")

    # drop duplicates
    dfT = dfT.drop_duplicates(keep="last")

    # sorting by date
    dfT = dfT.sort_values(by=["lab_id", "test_id", "date_testing"])

    # output combined dataframe
    dfT.to_csv(output, sep="\t", index=False)
    logger.info(f"Data successfully aggregated and saved in: {output}")
