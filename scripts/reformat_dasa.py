## -*- coding: utf-8 -*-

## Created by: Anderson Brito
## Email: anderson.brito@itps.org.br
## Release date: 2022-01-19
## Last update: 2023-09-01
## Refactor by Bragatte e João Pedro

import pandas as pd
import os
import numpy as np
import hashlib
import time
import argparse
from epiweeks import Week
from tqdm import tqdm ## add to requirements
import re
from utils import has_something_to_be_done

import warnings
import logging
warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.simplefilter(action='ignore', category=pd.errors.PerformanceWarning)

pd.set_option('display.max_columns', 500)
pd.options.mode.chained_assignment = None

today = time.strftime('%Y-%m-%d', time.gmtime()) ##for snakefile

def get_epiweeks(date):
    try:
        date = pd.to_datetime(date)
        epiweek = str(Week.fromdate(date, system="cdc")) # get epiweeks
        year, week = epiweek[:4], epiweek[-2:]
        epiweek = str(Week(int(year), int(week)).enddate())
    except:
        epiweek = ''
    return epiweek

def fix_datatable(df):
    logger.info(f"Fixing DataFrame with {df.shape[0]} rows and {df.shape[1]} columns")
    logger.info(df.columns.tolist())

    dfN = df

    # Rename columns
    lab_id = 'DASA'
    df = rename_columns(lab_id, df)

    logger.debug(f"The test with the lower number of tests (pathogens) has {df[['pathogen', 'test_id']].groupby('test_id').count().min()} tests")

    # Cast dates to datetime
    logger.info(f"Converting date columns to datetime")
    
    try:
        df['date_testing'] = pd.to_datetime(df['date_testing'], format='%Y-%m-%d')
    except:
        logger.info("Failed to convert date_testing to datetime. Trying another format.")
        df['date_testing'] = pd.to_datetime(df['date_testing'], format='%d/%m/%Y')

    # Drop rows with null values in test_id
    previous_rows = df.shape[0]
    df = df.dropna(subset=['test_id'])
    df = df[df['test_id'] != '']
    logger.info(f"Dropped {previous_rows - df.shape[0]} rows with null values in test_id")

    if 'pathogen' in df.columns.tolist(): ##column with unique row data
        id_columns = [
            'test_id',
            'age',
            'sex',
            'date_testing',
            'location',
            'state'
            ]

        dfN = pd.DataFrame() ## create empty dataframe, and populate it with reformatted data from original lab dataframe

        for column in id_columns:
            if column not in df.columns.tolist():
                df[column] = ''
                print('\t\t\t - No \'%s\' column found. Please check for inconsistencies. Meanwhile, an empty \'%s\' column was added.' % (column, column))

        ## missing columns
        df['Ct_geneE'] = ''
        df['Ct_ORF1ab'] = ''
        df['Ct_geneN'] = ''
        df['Ct_geneS'] = ''
        df['Ct_VSR'] = ''
        df['geneS_detection'] = ''
        df['Ct_RDRP'] = ''
        df['Ct_FluA'] = ''   
        df['Ct_FluB'] = ''

        ## generate sample_id and test_kit columns
        df.insert(1, 'sample_id', '')
        df.insert(1, 'test_kit', 'unknown') # Set test_kit to unknown, it will be updated later
        df.fillna('', inplace=True)

        ## assign id and deduplicate
        df, dfN = deduplicate(df, dfN, id_columns)

        if df.empty:
            return dfN

        logger.info("Pathogens found: " + "; ".join(df['pathogen'].unique()))

        ## starting lab specific reformatting
        pathogens = {
            'SC2': ['COVID'], 
            'FLUA': ['INFLUENZA_A', 'FLUA'], 
            'FLUB': ['INFLUENZA_B', 'FLUB'], 
            'VSR': ['SINCICIAL', 'VSR'], 
            'META': ['METAPNEUMOVIRUS'], 
            'RINO': ['RINOVIRUS'],
            'PARA': ['PARAINFLUENZA'], 
            'ADENO': ['ADENOVIRUS'], 
            'BOCA': ['BOCAVIRUS'], 
            'COVS': ['CORONAVIRUS_229', 'CORONAVIRUS_43', 'CORONAVIRUS_63', 'CORONAVIRUS_HKU1'], 
            'ENTERO': ['ENTEROVIRUS'], 
            'BAC': []
            }
        unique_cols = list(set(df.columns.tolist()))

        for i, (code, dfR) in enumerate(df.groupby('test_id')):
            data = {} ## one data row for each request
            for col in unique_cols:
                data[col] = dfR[col].tolist()[0]

            target_pathogen = {}
            for p, t in pathogens.items():
                data[p + '_test_result'] = 'NT' #'Not tested'
                for g in t:
                    target_pathogen[g] = p

            dfR['pathogen'] = dfR['pathogen'].apply(lambda x: target_pathogen[x])
            
            for virus, dfG in dfR.groupby('pathogen'):
                for idx, row in dfG.iterrows():
                    gene = dfG.loc[idx, 'pathogen']
                    ct_value = int(dfG.loc[idx, 'positivo'])
                    data[gene] = str(ct_value)
                    if ct_value == 1: # if Ct = 1
                        result = 'DETECTADO'
                        data[virus + '_test_result'] = 'Pos' #result
                    else: # if Ct = 0
                        result = 'NÃO DETECTADO'
                        data[virus + '_test_result'] = 'Neg' #result

            # Create test_kit for the test_id
            row_pathogens = dfR['pathogen'].unique().tolist()
            if len(row_pathogens) == 4:
                data['test_kit'] = 'test_4'
            elif len(row_pathogens) == 11:
                data['test_kit'] = 'test_14'
            else:
                raise Exception(f"Unexpected number of pathogens when creating test_kit: {len(row_pathogens)} ({row_pathogens})")
            
            # Add row data to dataframe
            dfN = pd.concat([dfN, pd.DataFrame(data, index=[0])], ignore_index=True)

        # Check if all test_kit values are set
        assert 'unknown' not in dfN['test_kit'].unique().tolist(), "Some test_kit values are still unknown"

    elif 'Gene S' in df.columns.tolist():
        ################################ DEPRECATED ################################
        if 'resultado' not in df.columns.tolist():
            if 'resultado_norm' in df.columns.tolist():
                df.rename(columns={'resultado_norm': 'resultado'}, inplace=True)
            else:
                if 'resultado_original' in df.columns.tolist():
                    df.rename(columns={'resultado_original': 'resultado'}, inplace=True)
                    df['resultado'] = df['resultado'].apply(lambda x: 'Neg' if x == 'NDT' else 'Pos')
                else:
                    print('No \'result\' column found.')
                    exit()

        if 'requisicao' not in df.columns.tolist():
            if 'codigo_externo_do_paciente' in df.columns.tolist():
                df.rename(columns={'codigo_externo_do_paciente': 'requisicao'}, inplace=True)
            else:
                df.insert(1, 'requisicao', '')
                print('\t\t\t - No \'requisicao\' column found. Please check for inconsistencies. Meanwhile, an empty \'requisicao\' column was added.')

        # print(dfL.columns.tolist())
        id_columns = ['requisicao', 'date_testing', 'age', 'sex', 'location', 'state', 'Gene N', 'Gene ORF', 'Gene S']
        for column in id_columns:
            if column not in df.columns.tolist():
                df[column] = ''
                print('\t\t\t - No \'%s\' column found. Please check for inconsistencies. Meanwhile, an empty \'%s\' column was added.' % (column, column))

        ## generate sample id
        df.insert(1, 'sample_id', '')
        df.insert(1, 'test_kit', 'thermo')
        df.fillna('', inplace=True)

        ## adding missing columns
        df['birthdate'] = ''
        df['Ct_FluA'] = ''
        df['Ct_FluB'] = ''
        df['Ct_VSR'] = ''
        df['Ct_RDRP'] = ''
        df['Ct_geneE'] = ''

        ## assign id and deduplicate
        df, dfN = deduplicate(df, dfN, id_columns)
        # print('3')
        # print(dfL.head())

        # print(dfL)
        if df.empty:
            # print('# Returning an empty dataframe')
            return dfN

        ## starting lab specific reformatting
        pathogens = {
            'FLUA': [],
            'FLUB': [],
            'VSR': [],
            'SC2': [],
            'META': [],
            'RINO': [],
            'PARA': [],
            'ADENO': [],
            'BOCA': [],
            'COVS': [],
            'ENTERO': [],
            'BAC': [],
            }

        # target_pathogen = {}
        for p, t in pathogens.items():
            if p != 'SC2':
                df[p + '_test_result'] = 'NT' #'Not tested'

        def not_assigned(geo_data):
            empty = [
                '',
                'SEM CIDADE',
                'MUDOU',
                'NAO_INFORMADO',
                'NAOINFORMADO',
                ]
            if geo_data in empty:
                geo_data = ''
            return geo_data

        df['location'] = df['location'].apply(lambda x: not_assigned(x))
        df['state'] = df['state'].apply(lambda x: not_assigned(x))

        for idx, row in df.iterrows():
            result = df.loc[idx, 'resultado']
            if result == 'NAO DETECTADO':
                df.loc[idx, 'Gene N'] = ''
                df.loc[idx, 'Gene ORF'] = ''
                df.loc[idx, 'Gene S'] = ''
            else: # if not reported
                result = 'NA'
                df.loc[idx, 'Gene N'] = str(round(float(df.loc[idx, 'Gene N']), 1))
                df.loc[idx, 'Gene ORF'] = str(round(float(df.loc[idx, 'Gene ORF']), 1))
                df.loc[idx, 'Gene S'] = str(round(float(df.loc[idx, 'Gene S']), 1))

        dfN = df
    else:
        print('\t\tWARNING! Unknown file format. Check for inconsistencies.')
        exit()
        
    return dfN


if __name__ == '__main__':
    FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logger = logging.getLogger("DASA ETL")
    # add handler to stdout
    handler = logging.StreamHandler()
    # Logger all levels
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(FORMAT)
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)

    parser = argparse.ArgumentParser(
        description="Performs diverse data processing tasks for specific DASA lab cases. It seamlessly loads and combines data from multiple sources and formats into a unified dataframe. It applies renaming and correction rules to columns, generates unique identifiers, and eliminates duplicates based on prior data processing. Age information is derived from birth dates, and sex information is adjusted accordingly. The resulting dataframe is sorted by date and saved as a TSV file. Duplicate rows are also identified and saved separately for further analysis.",
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

    logger.info(f"Starting DASA ETL")
    logger.info(f"Input folder: {input_folder}")
    logger.info(f"Rename file: {rename_file}")
    logger.info(f"Correction file: {correction_file}")
    logger.info(f"Cache file: {cache_file}")
    logger.info(f"Output file: {output}")

## local run
    # path = "/Users/*/respat/"
    # input_folder = path + 'data/'
    # rename_file = input_folder + 'rename_columns.xlsx'
    # correction_file = input_folder + 'fix_values.xlsx'
    # cache_file = input_folder + 'combined_cache.tsv'
    # output = input_folder + today + '_combined_dasa_test.tsv'

    lab_data_folder = input_folder + 'DASA/'
    if not has_something_to_be_done(lab_data_folder):
        logger.info(f"No files found in {lab_data_folder}")
        if cache_file not in [np.nan, '', None]: 
            logger.info(f"Just copying {cache_file} to {output}")
            os.system(f"cp {cache_file} {output}")
        else:
            logger.info(f"No cache file found. Nothing to be done.")
        exit()


    def load_table(file):
        df = ''
        if str(file).split('.')[-1] == 'tsv':
            separator = '\t'
            df = pd.read_csv(file, encoding='utf-8', sep=separator, dtype='str')
        elif str(file).split('.')[-1] == 'csv':
            separator = ','
            df = pd.read_csv(file, encoding='utf-8', sep=separator, dtype='str')
        elif str(file).split('.')[-1] in ['xls', 'xlsx']:
            df = pd.read_excel(file, index_col=None, header=0, sheet_name=0, dtype='str')
            df.fillna('', inplace=True)
        elif str(file).split('.')[-1] == 'parquet':
            df = pd.read_parquet(file, engine='auto')
            df.fillna('', inplace=True)
        else:
            print('Wrong file format. Compatible file formats: TSV, CSV, XLS, XLSX, PARQUET')
            exit()
        return df
    # print('Done load tables')

    ## load cache file
    if cache_file not in [np.nan, '', None]:
        dfT = load_table(cache_file)
        dfT.fillna('', inplace=True)
    else:
        # dfP = pd.DataFrame()
        dfT = pd.DataFrame()
    # print('Done load cache')

    ## load renaming patterns
    dfR = load_table(rename_file)
    dfR.fillna('', inplace=True)

    dict_rename = {}
    # dict_corrections = {}
    for idx, row in dfR.iterrows():
        id = dfR.loc[idx, 'lab_id']
        if id not in dict_rename:
            dict_rename[id] = {}
        old_colname = dfR.loc[idx, 'column_name']
        new_colname = dfR.loc[idx, 'new_name']
        rename_entry = {old_colname: new_colname}
        dict_rename[id].update(rename_entry)

    ## load value corrections
    dfC = load_table(correction_file)
    dfC.fillna('', inplace=True)
    dfC = dfC[dfC['lab_id'].isin(["DASA", "any"])] ## filter to correct data into fix_values DASA

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
    # print('Load rename_columns')

    def generate_id(column_id):
        id = hashlib.sha1(str(column_id).encode('utf-8')).hexdigest()
        return id
    # print('Done hashlib')


    def deduplicate(dfL, dfN, id_columns):
        # generate sample id
        dfL['unique_id'] = dfL[id_columns].astype(str).sum(axis=1)  ## combine values in rows as a long string
        dfL['sample_id'] = dfL['unique_id'].apply(lambda x: generate_id(x)[:16])  ## generate alphanumeric sample id with 16 characters

        ## prevent reprocessing of previously processed samples
        if cache_file not in [np.nan, '', None]:
            duplicates = set(dfL[dfL['sample_id'].isin(dfT['sample_id'].tolist())]['sample_id'].tolist())
            if len(duplicates) == len(set(dfL['sample_id'].tolist())):
                print('\n\t\t * ALL samples were already previously processed. All set!')
                dfN = pd.DataFrame()  ## create empty dataframe, and populate it with reformatted data from original lab dataframe
                dfL = pd.DataFrame()

                return dfN, dfL
            else:
                print('\n\t\t * A total of %s out of %s samples were already previously processed.' % (str(len(duplicates)), str(len(set(dfL['sample_id'].tolist())))))
                new_samples = len(set(dfL['sample_id'].tolist())) - len(duplicates)
                print('\t\t\t - Processing %s new samples...' % (str(new_samples)))
                dfL = dfL[~dfL['sample_id'].isin(dfT['sample_id'].tolist())]  ## remove duplicates
        else:
            new_samples = len(dfL['sample_id'].tolist())
            print('\n\t\t\t - Processing %s new samples...' % (str(new_samples)))

        return dfL, dfN

    def rename_columns(id, df):
        # print(df.columns.tolist())
        # print(dict_rename[id])
        if id in dict_rename:
            df = df.rename(columns=dict_rename[id])
        return df

    # fix data points
    def fix_data_points(id, col_name, value):
        new_value = value
        if value in dict_corrections[id][col_name]:
            new_value = dict_corrections[id][col_name][value]
        return new_value
    print('Done rename')

    ## open data files
    for sub_folder in os.listdir(input_folder):
        if sub_folder == 'DASA': ## check if folder is the correct one
            id = sub_folder
            sub_folder = sub_folder + '/'

            if not os.path.isdir(input_folder + sub_folder):
                logger.error(f"Folder {input_folder + sub_folder} not found.")
                break

            logger.info(f"Processing DataFrame from: {id}")

            for file_i, filename in enumerate(sorted(os.listdir(input_folder + sub_folder))):

                if not filename.endswith( ('.tsv', '.csv', '.xls', '.xlsx', '.parquet') ):
                    continue
                if filename.startswith( ('~', '_') ):
                    continue
                
                logger.info(f"Loading data from: {input_folder + sub_folder + filename}")
                logger.info(f"File {file_i + 1} of {len(os.listdir(input_folder + sub_folder))}")

                df = load_table(input_folder + sub_folder + filename)
                df.fillna('', inplace=True)
                df.reset_index(drop=True)

                logger.info(f"Removed duplicates. New shape: {df.shape[0]} rows and {df.shape[1]} columns")

                logger.info(f"Starting to fix DataFrame - {filename}")
                df = fix_datatable(df)
                logger.info(f"Finished fixing DataFrame - {filename}")
                logger.info(f"New shape: {df.shape[0]} rows and {df.shape[1]} columns")

                if df.empty:
                    logger.warning(f"Empty DataFrame after fixing - {filename}. Check for inconsistencies.")
                    continue

                df.insert(0, 'lab_id', id)
                # df = rename_columns(id, df) # fix data points
                dfT = dfT.reset_index(drop=True)
                df = df.reset_index(drop=True)

                logger.info(f"Starting to fix values - {filename}")

                # Joining the generic corrections with the lab-specific ones
                dict_corrections_full = {**dict_corrections['DASA'], **dict_corrections['any']}
                df = df.replace(dict_corrections_full)
                
                logger.info(f"Finished fixing values - {filename}")
                logger.info(f"New shape: {df.shape[0]} rows and {df.shape[1]} columns")
                logger.info(f"Starting to aggregate results - {filename}")                

                frames = [dfT, df]
                df2 = pd.concat(frames).reset_index(drop=True)
                dfT = df2

    logger.info(f"Finished aggregating results. New shape: {dfT.shape[0]} rows and {dfT.shape[1]} columns")
    dfT = dfT.reset_index(drop=True)
    dfT.fillna('', inplace=True)
    # print('Done fix tables')

    ## reformat dates and get ages
    dfT['date_testing'] = pd.to_datetime(dfT['date_testing'])

    dfT['epiweek'] = dfT['date_testing'].apply(lambda x: get_epiweeks(x))

    ## Add gene detection results
    def check_detection(ctValue):
        try:
            if ctValue[0].isdigit() and float(ctValue) > 0:
                result = 'SGTP' #'Detected'
            elif ctValue[0].isdigit() and float(ctValue) < 1:
                result = 'SGTF' #'Not detected'
            else:
                result = 'NA'
        except:
            result = ''
            pass
        return result


    ## Ct value columns
    targets = []
    for col in dfT.columns.tolist():
        if col == 'Ct_geneS':
            new_col = col.split('_')[1] + '_detection'
            if new_col not in targets:
                targets.append(new_col)

            dfT[new_col] = dfT[col].apply(lambda x: check_detection(x))


    ## reset index
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
    

    # possible sex values [nan, 'MASCULINO', 'FEMININO', 'F', 'M', 'I', 'None', 'INDETERMINADO']
    logger.info("Start fixing SEX and AGE columns")
    sex_mapping = {
        'MASCULINO': 'M',
        'FEMININO': 'F',
        'F': 'F',
        'M': 'M',
        'I': '',
        'None': '',
        'INDETERMINADO': ''
    }
    dfT['sex'] = dfT['sex'].replace(sex_mapping)

    pattern_float = '(\d+\.\d+)'
    pattern_int = '(\d+)'

    dfT['age'] = dfT['age'].astype('str').apply(
        lambda x: re.findall(pattern_float, x)[0] 
        if re.findall(pattern_float, x) else 
        re.findall(pattern_int, x)[0] 
        if re.findall(pattern_int, x) 
        else '-1'
    ).fillna('-1').astype('int')
    dfT['age'] = dfT['age'].apply(lambda x: -1 if x > 120 else x)

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


    ## output duplicates rows
    duplicates = dfT.duplicated().sum()
    if duplicates > 0:
        mask = dfT.duplicated(keep=False) # find duplicates
        dfD = dfT[mask]
        output2 = input_folder + 'duplicates.tsv'
        dfD.to_csv(output2, sep='\t', index=False)
        print('\nWARNING!\nFile with %s duplicate entries saved in:\n%s' % (str(duplicates), output2))

    ## drop duplicates
    dfT = dfT.drop_duplicates(keep='last')

    ## sorting by date
    dfT = dfT.sort_values(by=['lab_id', 'test_id', 'date_testing'])

    ## output combined dataframe
    dfT.to_csv(output, sep='\t', index=False)
    logger.info(f"Finished processing. Output file: {output}")
