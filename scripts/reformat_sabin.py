## -*- coding: utf-8 -*-

## Created by: Bragatte
## Email: marcelo.bragatte@itps.org.br
## Release date: 2022-01-19
## Last update: 2023-07-04

import pandas as pd
import os
import numpy as np
import hashlib
import time
import argparse
from epiweeks import Week
from tqdm.auto import tqdm
from typing import Any
import re

from utils import aggregate_results, has_something_to_be_done, LoggerSingleton

import warnings
import logging
warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.simplefilter(action='ignore', category=pd.errors.PerformanceWarning)

pd.set_option('display.max_columns', 500)
pd.options.mode.chained_assignment = None


today = time.strftime('%Y-%m-%d', time.gmtime()) ## for snakefile

def load_table(file):
    """
    Load data from file automatic respecting the file format. 
    Compatible file formats: TSV, CSV, XLS, XLSX, PARQUET

    Args:
        file (str): File path

    Returns:
        pandas Dataframe or str: Dataframe or empty string if file format is not compatible.
    """    

    df = ''
    if str(file).split('.')[-1] == 'tsv':
        separator = '\t'
        df = pd.read_csv(file, encoding='utf-8', sep=separator, dtype='str')
    elif str(file).split('.')[-1] == 'csv':
        separator = ','
        df = pd.read_csv(file, encoding='utf-8', sep=separator, dtype='str')
    elif str(file).split('.')[-1] in ['xls', 'xlsx']:
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
    
    elif str(file).split('.')[-1] == 'parquet':
        df = pd.read_parquet(file, engine='auto')
        df.fillna('', inplace=True)
    else:
        print('Wrong file format. Compatible file formats: TSV, CSV, XLS, XLSX, PARQUET')
        exit()
        
    return df


def get_epiweeks(date):
    """Replace the date by its epidemiological week

    Args:
        date (datetime): date to be replaced

    Returns:
        str: Epidemiological week id or empty string if date is invalid
    """        

    try:
        date = pd.to_datetime(date)
        epiweek = str(Week.fromdate(date, system="cdc")) ## get epiweeks
        year, week = epiweek[:4], epiweek[-2:]
        epiweek = str(Week(int(year), int(week)).enddate())
    except:
        epiweek = ''
    return epiweek


def generate_id(value):
    """Returns a hash for a given value

    Args:
        value (str): value to be hashed 

    Returns:
        str: sha1 hash hexdigest
    """ 
    return hashlib.sha1(str(value).encode('utf-8')).hexdigest()

tqdm.pandas()
def convert_date_column(df: pd.DataFrame, column_name: str) -> None:
    original_dates = df[column_name].copy()
    def convert_date(date: Any) -> str:
        try:
            return pd.to_datetime(date, format='%d/%m/%Y', dayfirst=True).strftime('%d/%m/%Y')
        except ValueError:
            try:
                return pd.to_datetime(date).strftime('%d/%m/%Y')
            except Exception as e:
                logging.warning(f"Failed to convert date {date}: {e}")
                return date
    df[column_name] = df[column_name].progress_apply(convert_date)
    modified_rows = df[original_dates != df[column_name]].index
    if len(modified_rows) > 0:
        logging.info(f"Modified rows for column {column_name}: {modified_rows.tolist()}")

def fix_datatable(df):
    """
    Fixes dataframe errors. 
    Adds pathogen _test_result columns, test_kit column, and performs othe fixes.

    Args:
        df (pandas dataframe): dataframe to be fixed

    Returns:
        pandas dataframe: fixed dataframe
    """        

    FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logger = LoggerSingleton().get_logger("SABIN FIX DATATABLE")

    if 'OS' not in df.columns.tolist():
        logger.warning("Unknown file format. Check for inconsistencies.")
        return df
    
    logger.info("Fixing dtypes and handling dates")

# define columns dtypes to reduce the use of memory
    df["OS"] = df["OS"].astype('str')
    df["Código Posto"] = df["Código Posto"].astype('int16')
    df["Estado"] = df["Estado"].astype('str')
    df["Municipio"] = df["Municipio"].astype('str')

    # convert_date_column( df, "DataAtendimento" )
    # convert_date_column( df, "DataNascimento" )
    # convert_date_column( df, "DataAssinatura" )

    # if data is in format dd/mm/yy replace by dd/mm/yyyy
    # replace by 2020, 2021, 2022 or 2023
    format = r"\d{2}/\d{2}/\d{2}$"
    reformat_year_function = lambda x: x if re.match(format, x) is None else x[:-2] + "20" + x[-2:]
    df["DataAtendimento"] = df["DataAtendimento"].apply(
        reformat_year_function
    )
    df["DataNascimento"] = df["DataNascimento"].apply(
        reformat_year_function
    )
    df["DataAssinatura"] = df["DataAssinatura"].apply(
        reformat_year_function
    )

    # convert dates ad %d/%m/%Y
    df["DataAtendimento"] = pd.to_datetime(df["DataAtendimento"], format='%d/%m/%Y', dayfirst=True)
    df["DataNascimento"] = pd.to_datetime(df["DataNascimento"], format='%d/%m/%Y', dayfirst=True, errors='coerce')
    df["DataAssinatura"] = pd.to_datetime(df["DataAssinatura"], format='%d/%m/%Y', dayfirst=True)

    # show head of date columns
    logger.info(f"DataAtendimento - {df['DataAtendimento'].min()} - {df['DataAtendimento'].max()}")
    logger.info(f"DataNascimento - {df['DataNascimento'].min()} - {df['DataNascimento'].max()}")
    logger.info(f"DataAssinatura - {df['DataAssinatura'].min()} - {df['DataAssinatura'].max()}")

    df["Sexo"] = df["Sexo"].astype('str')
    df["Descricao"] = df["Descricao"].astype('str')
    df["Parametro"] = df["Parametro"].astype('str')
    df["Resultado"] = df["Resultado"].astype('str')

    ## add sample_id and test_kit 
    df.insert(1, 'sample_id', '')
    df.insert(1, 'test_kit', '')

    logger.info("Finished fixing dtypes and handling dates")

    id_columns = [
        'OS', 
        'Estado', 
        'Municipio', 
        'DataAtendimento', 
        'Sexo', 
        'DataNascimento', 
        'Descricao'
    ]

    for column in id_columns:
        if column not in df.columns.tolist():
            df[column] = ''
            logger.warning(f"No '{column}' column found. Please check for inconsistencies. Meanwhile, an empty '{column}' column was added.")

    logger.info("Start deduplicating dataframe")
    dfN = df
    df, dfN = deduplicate(df, dfN, id_columns)
    if df.empty:
        return df

    # Test Kit Covid
    # Test Kit 21 -> Painel Molecular

    logger.info("Starting creating test_kit")

    PARAMETERS_21_TESTS = {
        # PAINCOVI
        'PARA1','PARA2', 'PARA3','PARA4',
        'BORDETELLAP','VSINCICIAL','CPNEUMONIAE',
        'ADEN','CORON','CORHKU','CORNL','CORC',
        'HUMANMET','HUMANRH','INFLUEH','INFLUEN','INFLUENZ','INFLUEB',
        'MYCOPAIN','PAINSARS','RSPAIN',
    }

    PARAMETERS_24_TESTS = {
        # RESPIRA
        'HPIV1', 'HPIV2', 'HPIV3', 'HPIV4',
        'RSVA', 'RSVB', 'MPVR', 'HRV', 
        'HBOV', 'HEVR', 'ADEV', 'BPP', 
        'BP', 'CP', 'MP', 'HI',
        'LP', 'SP', 'NL63', 'OC43', 'COR229E', 
        'H1N1R', 'H1PDM09', 'H3', 'INFLUA', 'INFLUB',
    }

    PARAMETERS_4_TESTS = {
        # PCRESPSL & PCRVRESP
        'PCRVRESPBM', 'PCRVRESPBM2', 'PCRVRESPBM3', 'PCRVRESPBM4',
    }

    PARAMETERS_COVID_PCR = {
        'NALVO', 'PCRSALIV', 'TMR19RES1', 'NALVOSSA',
        'RDRPALVOCTL', 'RDRPALVO',
    }

    PARAMETERS_COVID_ANTIGEN = {
        'COVIDECO'
    }

    df["test_kit"] = df["Parametro"].apply(
        lambda x: 
            "covid_antigen" 
            if x in PARAMETERS_COVID_ANTIGEN
            else "covid_pcr" 
            if x in PARAMETERS_COVID_PCR
            else "test_21"
            if x in PARAMETERS_21_TESTS
            else "test_24"
            if x in PARAMETERS_24_TESTS
            else "test_4"
            if x in PARAMETERS_4_TESTS
            else "unknown"
    )

    logger.info("Finished creating test_kit")

    # test_name = 'test_21' if 'test_21' in dfL['test_kit'].tolist() else 'covid'
    df.fillna('', inplace=True)

    ## adding missing columns
    if 'DataNascimento' not in df.columns.tolist():
        df['birthdate'] = ''

    df['Ct_FluA'] = ''
    df['Ct_FluB'] = ''
    df['Ct_VSR'] = ''
    df['Ct_RDRP'] = ''
    df['Ct_geneE'] = ''
    df['Ct_geneN'] = ''
    df['Ct_geneS'] = ''
    df['Ct_ORF1ab'] = ''
    df['geneS_detection'] = ''


    # Removing unnecessary parameters
    df = (
        df

        # SARS-COV2
        # Remove parameters 'RDRPCI', 'NALVOCI', 'NALVOCQ', 'NALVOCTL', 'RDRPALVOCTL'
        # These parameters are used in internal control
        .query("Parametro not in ('RDRPCI', 'NALVOCI', 'NALVOCQ', 'NALVOCTL', 'RDRPALVOCTL')")

        # PCRESPSL
        # Remove parameters 'PCRESPSL' and 'PCRVRESP'
        .query("Parametro not in ('PCRESPSL', 'PCRVRESP')")
        
        # PCRESPSL & PCRVRESP
        # These parametes are summarized by the 'PCRVRESPBM' parameter
        .query("Parametro not in ('GENES', 'GENERDRP', 'GENEN')")

        # RESPIRA
        # Remove parameters RESPIRA1, RESPIRA2, RESPIRA3, RESPIRA4
        .query("Parametro not in ('RESPIRA', 'RESPIRA1', 'RESPIRA2', 'RESPIRA3', 'RESPIRA4')")

    )

    logger.info("Calculating test results")

    # Fixing Result column on RESPIRA records
    # Negative if Resultado == '0'
    # Positive if Resultado has the name of the pathogen
    df['Resultado'] = df['Resultado'].mask(
        df['ExcelSheet'] == 'RESPIRA', 
        df['Resultado'].apply(lambda x: 'Pos' if x != '0' else 'Neg') 
    )

    PATHOGENS_PARAMETERS = {
        'SC2': {
            # All the parameters from the COVID-exclusive SABIN file
            'NALVO', 'PCRSALIV', 'COVIDECO', 'TMR19RES1', 'NALVOSSA',
            'RDRPALVO', 
            
            # PAINCOVI
            'PAINSARS', # SARS-COV2

            # PCRESPSL & PCRVRESP
            'PCRVRESPBM',  # SARS-COV2 # ==>  GENES+GENERDRP+GENEN
        },
        'FLUA':{
            # PAINCOVI
            'INFLUEH', # INFLUENZA A (H3N2)
            'INFLUEN', # INFLUENZA A (H1N1)
            'INFLUENZ', # INFLUENZA A (H1N1 - 2009)

            # RESPIRA
            'INFLUA','H1N1R', 'H1PDM09', 'H3',

            # PCRESPSL & PCRVRESP 
            'PCRVRESPBM2', # INFLUEZA A
        },
        'FLUB':{
            # PAINCOVI
            'INFLUEB', # INFLUENZA B

            # RESPIRA
            'INFLUB',

            # PCRESPSL & PCRVRESP
            'PCRVRESPBM3', # INFLUEZA B
        },
        'VSR':{
            # PAINCOVI
            'VSINCICIAL',

            # RESPIRA
            'RSVA', 'RSVB',

            # PCRESPSL & PCRVRESP
            'PCRVRESPBM4',
        },
        'META':{
            # PAINCOVI
            'HUMANMET',  # METAPNEUMOVÍRUS HUMANO

            # RESPIRA
            'MPVR',
        },
        'RINO':{
            # PAINCOVI
            'HUMANRH',   # RHINOVÍRUS HUMANO
            
            # RESPIRA
            'HRV',
        },
        'PARA':{
            # PAINCOVI
            'PARA1','PARA2','PARA3','PARA4',

            # RESPIRA   
            'HPIV1', 'HPIV2', 'HPIV3', 'HPIV4' 
        },
        'ADENO':{
            # PAINCOVI
            'ADEN', # ADENOVIRUS

            # RESPIRA
            'ADEV',
        },
        'COVS':{
            # PAINCOVI
            'CORON',       # CORONAVÍRUS 229E (?)
            'CORHKU',      # CORONAVÍRUS HKU1
            'CORNL',       # CORONAVÍRUS NL63
            'CORC',        # CORONAVÍRUS OC43

            # RESPIRA
            'NL63', 'OC43', 'COR229E',
        },
        'BAC':{
            # PAINCOVI
            'CPNEUMONIAE', # CLAMYDOPHILA PNEUMONIA
            'MYCOPAIN',    # MYCOPLASMA PNEUMONIAE
            'BORDETELLAP', # BORDETELLA PERTUSSIS
            'RSPAIN',      # BORDETELLA PARAPEERTUSSIS (IS1001)

            # RESPIRA
            'BPP',	# Bordetella parapertussis
            'BP',	# Bordetella pertussis
            'CP',	# Chlamydophila pneumoniae
            'MP',	# Mycoplasma pneumoniae
            'HI',	# Haemophilus in'f'luenza
            'LP',	# Legionella pneumophila
            'SP',	# Streptococcus pneumoniae
        },
        'BOCA':{
            # RESPIRA
            'HBOV',
        },
        'ENTERO':{
            # RESPIRA
            'HEVR',
        },
    }

    for pathogen, parameter_list in PATHOGENS_PARAMETERS.items():
        test_result = pathogen + '_test_result'
        df[test_result] = df.apply(
            lambda x: 'NT' if x['Parametro'] not in parameter_list else x['Resultado'], 
            axis=1
        )

    logger.info("Finished calculating test results")

    # Add missing test_result columns
    for pathogen in PATHOGENS_PARAMETERS.keys():
        test_result = pathogen + '_test_result'
        if test_result not in df.columns.tolist():
            df[test_result] = 'NT'
    
    # drop Resultado
    df = df.drop(columns=['Resultado'], errors='ignore')

    logger.info("Finished fix_datatables")

    return df


if __name__ == '__main__':
    FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logger = logging.getLogger("SABIN ETL")
    # add handler to stdout
    handler = logging.StreamHandler()
    # Logger all levels
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(FORMAT)
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)

    parser = argparse.ArgumentParser(
        description="Performs diverse data processing tasks for specific SABIN lab cases. It seamlessly loads and combines data from multiple sources and formats into a unified dataframe. It applies renaming and correction rules to columns, generates unique identifiers, and eliminates duplicates based on prior data processing. Age information is derived from birth dates, and sex information is adjusted accordingly. The resulting dataframe is sorted by date and saved as a TSV file. Duplicate rows are also identified and saved separately for further analysis.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("--datadir", required=True, help="Name of the folder containing independent folders for each lab")
    parser.add_argument("--rename", required=False, help="TSV, CSV, or excel file containing new standards for column names")
    parser.add_argument("--correction", required=False, help="TSV, CSV, or excel file containing data points requiring corrections")
    parser.add_argument("--cache", required=False, help="Previously processed data files")
    parser.add_argument("--output", required=True, help="TSV file aggregating all columns listed in the 'rename file'")
    args = parser.parse_args()

    path = os.path.abspath(os.getcwd())
    input_folder = path + '/' + args.datadir + '/'
    rename_file = args.rename
    correction_file = args.correction
    cache_file = args.cache
    output = args.output

    logger.info(f"Starting SABIN ETL")
    logger.info(f"Input folder: {input_folder}")
    logger.info(f"Rename file: {rename_file}")
    logger.info(f"Correction file: {correction_file}")
    logger.info(f"Cache file: {cache_file}")
    logger.info(f"Output file: {output}")

    lab_data_folder = input_folder + 'SABIN/'
    if not has_something_to_be_done(lab_data_folder):
        print(f"No files found in {lab_data_folder}")
        if cache_file not in [np.nan, '', None]: 
            print(f"Just copying {cache_file} to {output}")
            os.system(f"cp {cache_file} {output}")
        else:
            print(f"No cache file found. Nothing to be done.")
        print(f"Data successfully aggregated and saved in: {output}")
        exit()

    ## load cache file
    if cache_file not in [np.nan, '', None]:
        logger.info(f"Loading cache file: {cache_file}")

        dfT = load_table(cache_file)
        dfT.fillna('', inplace=True)
    else:
        logger.info(f"No cache file provided. Starting from scratch.")

        dfT = pd.DataFrame()

    ## load column renaming rules and build the rename dictionary
    dfR = load_table(rename_file)
    dfR.fillna('', inplace=True)

    dict_rename = {}
    for idx, row in dfR.iterrows():
        id = dfR.loc[idx, 'lab_id']
        if id not in dict_rename:
            dict_rename[id] = {}
        old_colname = dfR.loc[idx, 'column_name']
        new_colname = dfR.loc[idx, 'new_name']
        rename_entry = {old_colname: new_colname}
        dict_rename[id].update(rename_entry)

    ## load value corrections and build the correction dictionary
    dfC = load_table(correction_file)
    dfC.fillna('', inplace=True)
    dfC = dfC[dfC['lab_id'].isin(["SABIN", "any"])] ##filter to correct data into fix_values SABIN

    dict_corrections = {}
    all_ids = list(set(dfC['lab_id'].tolist()))
    for idx, row in dfC.iterrows():
        lab_id = dfC.loc[idx, 'lab_id']
        colname = dfC.loc[idx, 'column_name']

        old_data = dfC.loc[idx, 'old_data']
        new_data = dfC.loc[idx, 'new_data']
        if old_data + new_data not in ['']:
            labs = []
            if colname == 'any':
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

    ## open data files
    for sub_folder in os.listdir(input_folder):
        if sub_folder == 'SABIN': # check if folder is the correct one
            id = sub_folder
            sub_folder = sub_folder + '/'

            if not os.path.isdir(input_folder + sub_folder):
                logger.error(f"Folder {input_folder + sub_folder} not found.")
                break
            
            logger.info(f"Processing DataFrame from: {id}")

            for filename in sorted(os.listdir(input_folder + sub_folder)):
                
                if not filename.endswith( ('.tsv', '.csv', '.xls', '.xlsx', '.parquet') ):
                    continue
                if filename.startswith( ('~', '_') ):
                    continue

                logger.info(f"Loading data from: {input_folder + sub_folder + filename}")

                df_path = input_folder + sub_folder + filename
                df = load_table(df_path)
                df.fillna('', inplace=True)
                df.reset_index(drop=True)

                if filename.endswith('csv'):
                    # 20230823_SABIN_Painel respiratorio 2023ateSE33.xlsx - PAINCOVI.csv
                    # 20230823_SABIN_Painel respiratorio 2023ateSE33.xlsx - RESPIRA.csv
                    # 20230823_SABIN_Painel respiratorio 2023ateSE33.xlsx - PCRESPSL.csv
                    # 20230823_SABIN_Painel respiratorio 2023ateSE33.xlsx - PCRVRESP.csv
                    df['ExcelSheet'] = (
                        'PAINCOVI' if 'PAINCOVI' in filename 
                        else 'RESPIRA' if 'RESPIRA' in filename 
                        else 'PCRESPSL' if 'PCRESPSL' in filename 
                        else 'PCRVRESP' if 'PCRVRESP' in filename 
                        else 'COVID'
                    )

                logger.info(f"Loaded {df.shape[0]} rows and {df.shape[1]} columns")

                # Remove duplicates
                df = df.drop_duplicates(
                    subset=['OS', 'Descricao', 'Parametro'], 
                    keep='last'
                )

                logger.info(f"Removed duplicates. New shape: {df.shape[0]} rows and {df.shape[1]} columns")

                logger.info(f"Starting to fix DataFrame - {filename}")
                df = fix_datatable(df)
                logger.info(f"Finished fixing DataFrame - {filename}")
                logger.info(f"New shape: {df.shape[0]} rows and {df.shape[1]} columns")

                if df.empty:
                    logger.warning(f"Empty DataFrame after fixing - {filename}. Check for inconsistencies.")
                    continue

                df.insert(0, 'lab_id', id)
                df = df.rename(columns=dict_rename[id])

                # logger.info(f"Renamed columns - {df.columns}")

                dfT = dfT.reset_index(drop=True)
                df = df.reset_index(drop=True)  

                logger.info(f"Starting to fix values - {filename}")

                # Joining the generic corrections with the lab-specific ones
                dict_corrections_full = {**dict_corrections['SABIN'], **dict_corrections['any']}
                df = df.replace(dict_corrections_full)

                logger.info(f"Finished fixing values - {filename}")
                logger.info(f"New shape: {df.shape[0]} rows and {df.shape[1]} columns")
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
                    ]
                )

                logger.info(f"Finished aggregating results - {filename}")
                logger.info(f"New shape: {df.shape[0]} rows and {df.shape[1]} columns")

                # Calculate AGE from BIRTHDATE and DATE_TESTING
                # Replacing null values with 1700-01-01 and 2200-01-01 to avoid errors (age 500)
                df['birthdate'] = df['birthdate'].replace([np.nan, None, ''], '1800-01-01')
                df['date_testing'] = df['date_testing'].replace([np.nan, None, ''], '2200-01-01')
                
                logger.info(f"Calculating DATE TESTING - {df['date_testing'].min()} - {df['date_testing'].max()}")
                logger.info(f"Calculating BIRTHDATE - {df['birthdate'].min()} - {df['birthdate'].max()}")

                ## Calculate age, considering NaT, and round to 1 decimal; replace NaN with -1
                df['age'] = (pd.to_datetime(df['date_testing']) - pd.to_datetime(df['birthdate'])) 
                df['age'] = df['age'] / np.timedelta64(1, 'Y')
                df['age'] = df['age'].apply(lambda x: np.round(x, 1)) # round to 1 decimal
                df['age'] = df['age'].apply(lambda x: int(x))

                # Remove AGE values < 0 and > 150 -> absurd values created by the replacement of null values
                df['age'] = df['age'].apply(lambda x: x if x >= 0 and x <= 150 else -1)

                ## fix sex information
                df['sex'] = df['sex'].apply(lambda x: x[0] if x != '' else x)

                logger.info(f"Concatenating DataFrames - {filename}")

                df = df.reset_index(drop=True)
                dfT = dfT.reset_index(drop=True)

                frames = [dfT, df]
                df2 = pd.concat(frames, ignore_index=True).reset_index(drop=True)
                dfT = df2

                logger.info(f"Finished processing file: {filename}")
                    


    dfT = dfT.reset_index(drop=True)
    dfT.fillna('', inplace=True)
    # print('Done fix tables')

    ## old place where add `Fixing data points...`

    ## reformat dates and get ages
    dfT['date_testing'] = pd.to_datetime(dfT['date_testing'])

    dfT['epiweek'] = dfT['date_testing'].apply(lambda x: get_epiweeks(x))

    dfT = dfT.reset_index(drop=True)
    key_cols = [
        'lab_id',
        'test_id',
        'test_kit',
        'patient_id',
        'sample_id',
        'state',
        'location',
        'date_testing',
        'epiweek',
        'age',
        'sex',
        'FLUA_test_result',
        'Ct_FluA',
        'FLUB_test_result',
        'Ct_FluB',
        'VSR_test_result',
        'Ct_VSR',
        'SC2_test_result',
        'Ct_geneE',
        'Ct_geneN',
        'Ct_geneS',
        'Ct_ORF1ab',
        'Ct_RDRP',
        'geneS_detection',
        'META_test_result',
        'RINO_test_result',
        'PARA_test_result',
        'ADENO_test_result',
        'BOCA_test_result',
        'COVS_test_result',
        'ENTERO_test_result',
        'BAC_test_result'
    ]

    for col in dfT.columns.tolist():
        if col not in key_cols:
            dfT = dfT.drop(columns=[col])
    
    for col in key_cols:
        if col not in dfT.columns.tolist():
            logger.warning(f"Column {col} not found in the table. Adding it with empty values.")
            dfT[col] = ''

    dfT = dfT[key_cols]
    
    def date2str(value):
        try:
            value = value.strftime('%Y-%m-%d')
        except:
            value = ''
        return value

    dfT['date_testing'] = dfT['date_testing'].apply(lambda x: date2str(x))

    ## Saving duplicated entries in a separate file
    duplicates = dfT.duplicated().sum()
    if duplicates > 0:
        mask = dfT.duplicated(keep=False) # find duplicates
        dfD = dfT[mask]
        output2 = input_folder + 'duplicates.tsv'
        dfD.to_csv(output2, sep='\t', index=False)

        logger.warning(f"File with {duplicates} duplicate entries saved in: {output2}")

    dfT = dfT.drop_duplicates(keep='last')
    dfT = dfT.sort_values(by=['lab_id', 'test_id', 'date_testing'])

    dfT.to_csv(output, sep='\t', index=False)
    logger.info(f"Data successfully aggregated and saved in: {output}")