## -*- coding: utf-8 -*-

## Created by: Bragatte
## Email: marcelo.bragatte@itps.org.br
## Release date: 2023-04-10
## Last update: 2023-07-04

## Libs
import pandas as pd
import os
import numpy as np
import hashlib
import time
import argparse
from epiweeks import Week
from tqdm import tqdm ## add to requirements
from utils import aggregate_results, has_something_to_be_done

## Settings
import warnings
import logging
warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.simplefilter(action='ignore', category=pd.errors.PerformanceWarning)

pd.set_option('display.max_columns', 500)
pd.options.mode.chained_assignment = None

today = time.strftime('%Y-%m-%d', time.gmtime()) ##for snakefile

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

def get_epiweeks(date):
    try:
        date = pd.to_datetime(date)
        epiweek = str(Week.fromdate(date, system="cdc")) # get epiweeks
        year, week = epiweek[:4], epiweek[-2:]
        epiweek = str(Week(int(year), int(week)).enddate())
#             epiweek = str(Week.fromdate(date, system="cdc"))  # get epiweeks
#             epiweek = epiweek[:4] + '_' + 'EW' + epiweek[-2:]
    except:
        epiweek = ''
    return epiweek

def generate_id(column_id):
    id = hashlib.sha1(str(column_id).encode('utf-8')).hexdigest()
    return id

def fix_datatable(dfL):
    dfN = dfL
    # print(dfL.columns.tolist())
    # print(''.join(dfL.columns.tolist()))
    if 'Código Da Cápsula' in dfL.columns.tolist(): # and '' in dfL['Codigo'].tolist(): #column with unique row data
        test_name = "Hilab exams"

        print('\t\tDados test HILAB >> Correct format. Proceeding...')

        ## drop arboviruses and bHCG tests
        arboviruses = ['Dengue', 'Zika','bHCG']
        dfL = dfL[~dfL['Exame'].str.contains('|'.join(arboviruses))]
        
        ## define columns dtypes to reduce the use of memory
        dfL["Código Da Cápsula"] = dfL["Código Da Cápsula"].astype('str')
        dfL["Região Do Brasil"] = dfL["Região Do Brasil"].astype('str')
        dfL["Estado"] = dfL["Estado"].astype('str')
        dfL["Cidade"] = dfL["Cidade"].astype('str')
        dfL["Data Do Exame"] = pd.to_datetime(dfL["Data Do Exame"], dayfirst=True, format='%d/%m/%Y').apply(lambda s: s.strftime('%Y-%m-%d'))
        dfL["Sexo"] = dfL["Sexo"].astype('str')
        dfL["Idade"] = dfL["Idade"].replace('', np.nan).astype(int, errors='ignore')
        dfL["Exame"] = dfL["Exame"].astype('str')
        dfL["Resultado"] = dfL["Resultado"].astype('str')

        ## change negative ages to NaN
        dfL.loc[dfL.fillna(0)['Idade'].astype(int) < 0, 'Idade'] = np.nan
        ## change ages greater than 200 to NaN
        dfL.loc[dfL.fillna(0)['Idade'].astype(int) > 127, 'Idade'] = np.nan
        dfL["Idade"] = dfL["Idade"].apply(lambda x: str(x).split('.')[0] if str(x).split('.')[0].isdigit() else x).astype(int, errors='ignore')

        ## add sample_id and test_kit
        dfL['sample_id'] = ''
        dfL['test_kit'] = ''

        ## set test_kit values for Covid-19 antigen tests
        covid_antigen_mask = dfL['Exame'] == 'Covid-19 Antígeno'
        dfL.loc[covid_antigen_mask, 'test_kit'] = 'covid_antigen'

        ## set test_kit values for flu tests
        flu_mask = dfL['Exame'].isin(['Influenza A', 'Influenza B'])
        dfL.loc[flu_mask, 'test_kit'] = 'flu_antigen'

        # ## set test_kit values for Dengue tests
        # dengue_mask = dfL['Exame'].str.contains('Dengue')
        # dfL.loc[dengue_mask, 'test_kit'] = 'dengue'

        dfL.fillna('', inplace=True)

        id_columns = [
            'Código Da Cápsula',
            'Região Do Brasil',
            'Estado',
            'Cidade',
            'Data Do Exame'
            ]

        for column in tqdm(id_columns):
            if column not in dfL.columns.tolist():
                dfL[column] = ''
                print('\t\t\t - No \'%s\' column found. Please check for inconsistencies. Meanwhile, an empty \'%s\' column was added.' % (column, column))

        ## adding missing columns
        dfL['Ct_FluA'] = ''
        dfL['Ct_FluB'] = ''
        dfL['Ct_VSR'] = ''
        dfL['Ct_RDRP'] = ''
        dfL['Ct_geneE'] = ''
        dfL['Ct_geneN'] = ''
        dfL['Ct_geneS'] = ''
        dfL['Ct_ORF1ab'] = ''
        dfL['geneS_detection'] = ''

        ## assign id and deduplicate
        dfL, dfN = deduplicate(dfL, dfN, id_columns, test_name)
        # print(df)

        if dfL.empty:
            #print('# Returning an empty dataframe')
            return dfN

        def assign_test_result(row):
            if row['Resultado'] == 'Reagente':
                return 'Pos'
            elif row['Resultado'] == 'Não Reagente':
                return 'Neg'
            else:
                return 'NT'
        
        # starting lab specific reformatting
        pathogens = {
            'SC2': ['Covid-19 Antígeno'], 
            'FLUA': ['Influenza A'],
            'FLUB': ['Influenza B'], 
            'VSR': [], 
            'META': [], 
            'RINO': [],
            'PARA': [], 
            'ADENO': [], 
            'BOCA': [], 
            'COVS': [], 
            'ENTERO': [], 
            'BAC': [],
            }

        for pathogen, tests in tqdm(pathogens.items()):
            if pathogen == 'SC2':
                mask = dfL['Exame'].isin(tests)
            else:
                mask = dfL['Exame'].isin(tests)

            dfL.loc[mask, pathogen + '_test_result'] = dfL.loc[mask].apply(assign_test_result, axis=1)
            dfL.loc[~mask, pathogen + '_test_result'] = 'NT'

        dfL = dfL.drop(columns=['Resultado'])
        return dfL
    else:
        #print('\t\tFile = ' + file)
        print('\t\tWARNING! Unknown file format. Check for inconsistencies.')
        #exit()
    return dfN

## Args
if __name__ == '__main__':
    FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logger = logging.getLogger("HILAB ETL")
    # add handler to stdout
    handler = logging.StreamHandler()
    # Logger all levels
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(FORMAT)
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)

    parser = argparse.ArgumentParser(
        description="Performs diverse data processing tasks for specific Hilab lab cases. It seamlessly loads and combines data from multiple sources and formats into a unified dataframe. It applies renaming and correction rules to columns, generates unique identifiers, and eliminates duplicates based on prior data processing. Age information is derived from birth dates, and sex information is adjusted accordingly. The resulting dataframe is sorted by date and saved as a TSV file. Duplicate rows are also identified and saved separately for further analysis.",
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

    lab_data_folder = input_folder + 'HILAB/'
    if not has_something_to_be_done(lab_data_folder):
        print(f"No files found in {lab_data_folder}")
        if cache_file not in [np.nan, '', None]: 
            print(f"Just copying {cache_file} to {output}")
            os.system(f"cp {cache_file} {output}")
        else:
            print(f"No cache file found. Nothing to be done.")
        print(f"Data successfully aggregated and saved in: {output}")
        exit()

    logger.info(f"Starting HILAB ETL")
    logger.info(f"Input folder: {input_folder}")
    logger.info(f"Rename file: {rename_file}")
    logger.info(f"Correction file: {correction_file}")
    logger.info(f"Cache file: {cache_file}")
    logger.info(f"Output file: {output}")

    if cache_file not in [np.nan, '', None]:
        logger.info(f"Loading cache file: {cache_file}")

        dfT = load_table(cache_file)
        dfT.fillna('', inplace=True)
    else:
        logger.info(f"No cache file provided. Starting from scratch.")

        dfT = pd.DataFrame()

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
    dfC = dfC[dfC['lab_id'].isin(["HILAB", "any"])] ## filter to correct data into fix_values DASA

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

    def deduplicate(dfL, dfN, id_columns, test_name):
        # generate sample id
        dfL['unique_id'] = dfL[id_columns].astype(str).sum(axis=1)  ## combine values in rows as a long string
        dfL['sample_id'] = dfL['unique_id'].apply(lambda x: generate_id(x)[:16])  ## generate alphanumeric sample id with 16 characters

        ## prevent reprocessing of previously processed samples
        if cache_file not in [np.nan, '', None]:
            duplicates = set(dfL[dfL['sample_id'].isin(dfT['sample_id'].tolist())]['sample_id'].tolist())
            if len(duplicates) == len(set(dfL['sample_id'].tolist())):
                print('\n\t\t * ALL samples (%s) were already previously processed. All set!' % test_name)
                dfN = pd.DataFrame()  ## create empty dataframe, and populate it with reformatted data from original lab dataframe
                dfL = pd.DataFrame()

                # print('1')
                # print(dfL.head())

                return dfN, dfL
            else:
                print('\n\t\t * A total of %s out of %s samples (%s) were already previously processed.' % (str(len(duplicates)), str(len(set(dfL['sample_id'].tolist()))), test_name))
                new_samples = len(set(dfL['sample_id'].tolist())) - len(duplicates)
                print('\t\t\t - Processing %s new samples...' % (str(new_samples)))
                dfL = dfL[~dfL['sample_id'].isin(dfT['sample_id'].tolist())]  ## remove duplicates
        else:
            new_samples = len(dfL['sample_id'].tolist())
            print('\n\t\t\t - Processing %s new samples (%s)...' % (str(new_samples), test_name))

        # print('2')
        # print(dfL.head())
        return dfL, dfN
    # print('Done cache file')

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
        if sub_folder == 'HILAB': ## check if folder is the correct one
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

                df = load_table(input_folder + sub_folder + filename)
                df.fillna('', inplace=True)
                df.reset_index(drop=True)

                logger.info(f"Loaded {df.shape[0]} rows and {df.shape[1]} columns")

                logger.info(f"Starting to fix DataFrame - {filename}")
                df = fix_datatable(df)
                logger.info(f"Finished fixing DataFrame - {filename}")
                logger.info(f"New shape: {df.shape[0]} rows and {df.shape[1]} columns")
                if df.empty:
                    logger.warning(f"Empty DataFrame after fixing - {filename}. Check for inconsistencies.")
                    continue

                df.insert(0, 'lab_id', id)
                df = rename_columns(id, df) # fix data points
                dfT = dfT.reset_index(drop=True)
                df = df.reset_index(drop=True)

                logger.info(f"Starting to fix values - {filename}")

                dict_corrections_full = {**dict_corrections['any']}
                df = df.replace(dict_corrections_full)

                logger.info(f"Finished fixing values - {filename}")
                logger.info(f"New shape: {df.shape[0]} rows and {df.shape[1]} columns")

                df = aggregate_results(
                    df, 
                    test_id_columns=[
                        'test_id', 'test_kit'
                    ], 
                    test_result_columns=[
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

                # checking duplicates
                # print(df.columns[df.columns.duplicated(keep=False)])
                # print(dfT.columns[dfT.columns.duplicated(keep=False)])

                ## add age from birthdate, if age is missing
                if 'birthdate' in df.columns.tolist():
                    for idx, row in tqdm(df.iterrows()):
                        birth = df.loc[idx, 'birthdate']
                        test = df.loc[idx, 'date_testing']
                        if birth not in [np.nan, '', None]:
                            birth = pd.to_datetime(birth)
                            test = pd.to_datetime(test) ## add to correct dtypes for calculations
                            age = (test - birth) / np.timedelta64(1, 'Y')
                            df.loc[idx, 'age'] = np.round(age, 1)                  ## this gives decimals
                            #df.loc[idx, 'age'] = int(age)
                        print(f'Processing tests {idx + 1} of {len(df)}')            ## print processed lines 

                        ## Change the data type of the 'age' column to integer
                        df['age'] = pd.to_numeric(df['age'], downcast='integer',errors='coerce').fillna(-1).astype(int)
                        df['age'] = df['age'].apply(int)

                ## fix sex information
                df['sex'] = df['sex'].apply(lambda x: x[0] if x != '' else x)

                frames = [dfT, df]
                df2 = pd.concat(frames).reset_index(drop=True)
                dfT = df2

                logger.info(f"Finished processing file: {filename}")

    dfT = dfT.reset_index(drop=True)
    dfT.fillna('', inplace=True)
    # print('Done fix tables')


## reformat dates and get ages
    dfT['epiweek'] = dfT['date_testing'].apply(lambda x: get_epiweeks(x))

## IF Molecular tests -> Add gene detection results
    def check_detection(ctValue):
        try:
            if ctValue[0].isdigit() and float(ctValue) > 0:
                result = 'Det' #'Detected'
            elif ctValue[0].isdigit() and float(ctValue) < 1:
                result = 'ND' #'Not detected'
            else:
                result = '' #''
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
        #'region',
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
        'BAC_test_result',
        ]

    for col in dfT.columns.tolist():
        if col not in key_cols:
            dfT = dfT.drop(columns=[col])

    for col in key_cols:
        if col not in dfT.columns.tolist():
            logger.warning(f"Column {col} not found in the table. Adding it with empty values.")
            dfT[col] = ''

    dfT = dfT[key_cols]

    duplicates = dfT.duplicated().sum()
    if duplicates > 0:
        mask = dfT.duplicated(keep=False) # find duplicates
        dfD = dfT[mask]
        output2 = input_folder + 'duplicates.tsv'
        dfD.to_csv(output2, sep='\t', index=False)
        logger.warning(f"File with {duplicates} duplicate entries saved in: {output2}")

    ## drop duplicates
    dfT = dfT.drop_duplicates(keep='last')

## sorting by date
    dfT = dfT.sort_values(by=['lab_id', 'test_id', 'date_testing'])

## time controller for optimization of functions `def`
    ## output combined dataframe
    dfT.to_csv(output, sep='\t', index=False)
    logger.info(f"Data successfully aggregated and saved in: {output}")