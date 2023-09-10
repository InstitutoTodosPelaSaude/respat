## -*- coding: utf-8 -*-

## Created by: Anderson Brito
## Email: anderson.brito@itps.org.br
## Release date: 2022-01-19
## Last update: 2023-07-04
## Refactor by: Bragatte

import pandas as pd
import os
import numpy as np
import hashlib
import time
import argparse
from epiweeks import Week
from tqdm.auto import tqdm

import logging
from utils import has_something_to_be_done

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.simplefilter(action='ignore', category=pd.errors.PerformanceWarning)

pd.set_option('display.max_columns', 500)
pd.options.mode.chained_assignment = None

today = time.strftime('%Y-%m-%d', time.gmtime()) ## for snakefile

def fix_datatable(df):
    dfN = df
    # print(dfL.columns.tolist())
    # print(''.join(dfL.columns.tolist()))
    # if lab == 'HLAGyn':

    # Drop duplicates on 'Pedido'
    df = df.drop_duplicates(subset=['Pedido'], keep='last')
    
    df['Idade'] = df['Idade'].where(df['Idade'] != '', '-1').astype(int)
    df = df.query('Idade >= 0 and Idade <= 120').reset_index(drop=True)

    df['Sexo'] = df['Sexo'].str.upper().map({"FEMININO": "F", "MASCULINO": "M"}).fillna("I")

    if 'H1N1' in ''.join(df.columns.tolist()) or 'Influenza' in ''.join(df.columns.tolist()): ## column with unique row data
        test_name = "Painel viral HLAGyn"

        # print('\t\tDados resp_vir >> Correct format. Proceeding...' % test_name)
        # add sample_id and test_kit
        df.insert(1, 'sample_id', '')
        df.fillna('', inplace=True)

        id_columns = [
            'Pedido',
            'Idade',
            'Sexo',
            'Data Coleta',
            'Cidade',
            'UF',
            ]

        test_columns = []
        if 'Parainfluenza' in ''.join(df.columns.tolist()):
            test_columns = [
                'VIRUS_Influenza A',
                'VIRUS_Influenza H1N1',
                'VIRUS_Influenza H3',
                'VIRUS_Influenza B',
                'VIRUS_Metapneumovírus',
                'VIRUS_Sincicial A',
                'VIRUS_Sincicial B',
                'VIRUS_Rinovírus',
                'VIRUS_Parainfluenza 1',
                'VIRUS_Parainfluenza 2',
                'VIRUS_Parainfluenza 3',
                'VIRUS_Parainfluenza 4',
                'VIRUS_Adenovirus',
                'VIRUS_Bocavirus',
                'VIRUS_CoV-229E',
                'VIRUS_CoV-HKU',
                'VIRUS_CoV-NL63',
                'VIRUS_CoV-OC43',
                'VIRUS_SARS_Like',
                'VIRUS_SARS-CoV-2',
                'VIRUS_Enterovírus',
                'BACTE_Bordetella pertussis',
                'BACTE_Bordetella parapertussis',
                'BACTE_Mycoplasma pneumoniae',
                ]
            df.insert(1, 'test_kit', 'test_24')
        elif 'PH4' in ''.join(df.columns.tolist()):
            test_columns = [
                'VIRUS_IA',
                'VIRUS_H1N1',
                'VIRUS_AH3',
                'VIRUS_B',
                'VIRUS_MH',
                'VIRUS_SA',
                'VIRUS_SB',
                'VIRUS_RH',
                'VIRUS_PH',
                'VIRUS_PH2',
                'VIRUS_PH3',
                'VIRUS_PH4',
                'VIRUS_ADE',
                'VIRUS_BOC',
                'VIRUS_229E',
                'VIRUS_HKU',
                'VIRUS_NL63',
                'VIRUS_OC43',
                'VIRUS_SARS',
                'VIRUS_COV2',
                'VIRUS_EV',
                'BACTE_BP',
                'BACTE_BPAR',
                'BACTE_MP',
                ]
            df.insert(1, 'test_kit', 'test_24')
        elif 'SARS-CoV-2' in ''.join(df.columns.tolist()):
            test_columns = [
                'Vírus Influenza A',
                'Vírus Influenza B',
                'Vírus Sincicial Respiratório A/B',
                'Coronavírus SARS-CoV-2',
                ]
            df.insert(1, 'test_kit', 'test_4')
        else:
            print('WARNING! Unknown file format. Check for inconsistencies.')

        for column in id_columns + test_columns:
            if column not in df.columns.tolist():
                df[column] = ''
                print('\t\t\t - No \'%s\' column found. Please check for inconsistencies. Meanwhile, an empty \'%s\' column was added.' % (column, column))


        ## assign id and deduplicate
        df, dfN = deduplicate(df, dfN, id_columns, test_name)
        # print('3')
        # print(dfL.head())

        if df.empty:
            # print('# Returning an empty dataframe')
            return dfN


        # starting reformatting process
        pathogens = {
            'SC2': [
                'VIRUS_SARS_Like',
                'VIRUS_SARS-CoV-2',
                'Coronavírus SARS-CoV-2',
                'VIRUS_SARS',
                'VIRUS_COV2',
                ],
            'FLUA': [
                'VIRUS_Influenza A',
                'VIRUS_Influenza H1N1',
                'VIRUS_Influenza H3',
                'Vírus Influenza A',
                'VIRUS_IA',
                'VIRUS_H1N1',
                'VIRUS_AH3',
                ],
            'FLUB': [
                'VIRUS_Influenza B',
                'Vírus Influenza B',
                'VIRUS_B',
                ],
            'VSR': [
                'VIRUS_Sincicial A',
                'VIRUS_Sincicial B',
                'Vírus Sincicial Respiratório A/B',
                'VIRUS_SA','VIRUS_SB',
                ],
            'META': [
                'VIRUS_Metapneumovírus',
                'VIRUS_MH',
                ],
            'RINO': [
                'VIRUS_Rinovírus',
                'VIRUS_RH',
                ],
            'PARA': [
                'VIRUS_Parainfluenza 1',
                'VIRUS_Parainfluenza 2',
                'VIRUS_Parainfluenza 3',
                'VIRUS_Parainfluenza 4',
                'VIRUS_PH',
                'VIRUS_PH2',
                'VIRUS_PH3',
                'VIRUS_PH4',
                ],
            'ADENO': [
                'VIRUS_Adenovirus',
                'VIRUS_ADE',
                ], 
            'BOCA': [
                'VIRUS_Bocavirus',
                'VIRUS_BOC',
                ],
            'COVS': [
                'VIRUS_CoV-229E',
                'VIRUS_CoV-HKU',
                'VIRUS_CoV-NL63',
                'VIRUS_CoV-OC43',
                'VIRUS_229E',
                'VIRUS_HKU',
                'VIRUS_NL63',
                'VIRUS_OC43',
                ],
            'ENTERO': [
                'VIRUS_Enterovírus',
                'VIRUS_EV',
                ],
            'BAC': [
                'BACTE_Bordetella pertussis',
                'BACTE_Bordetella parapertussis',
                'BACTE_Mycoplasma pneumoniae',
                'BACTE_BP',
                'BACTE_BPAR',
                'BACTE_MP',
                ]
            }

        # adding missing columns
        if 'Dt. Nascimento' not in df.columns.tolist():
            df['birthdate'] = ''
        df['Ct_FluA'] = ''
        df['Ct_FluB'] = ''
        df['Ct_VSR'] = ''
        df['Ct_RDRP'] = ''
        df['Ct_geneS'] = ''
        df['Ct_geneN'] = ''
        df['Ct_ORF1ab'] = ''
        df['Ct_geneE'] = ''
        df['patient_id'] = ''


        # Detectado, Não Detectado -> Neg, Pos
        df = df.replace({'Detectado': -10, 'Não Detectado': -20})
        for pathogen, pathogen_columns in pathogens.items():
            test_result = pathogen + '_test_result'

            # All test_columns that are in pathogen_columns
            pathogen_df_columns = list(set(test_columns).intersection(pathogen_columns))
            if pathogen_df_columns == []:
                df[test_result] = 'NT'
                continue

            df[test_result] = df[pathogen_df_columns].max(axis=1)

        # Replace -10 and -20 with Pos and Neg
        df = df.replace({-10: 'Pos', -20: 'Neg'}) 

        return df          

    elif 'CT_N' in df.columns.tolist():
        # print('\t\tDados covid >> Correct format. Proceeding...')
        test_name = "covid_pcr"

        id_columns = [
            'Pedido',
            'Idade',
            'Sexo',
            'Data Coleta',
            'Cidade',
            'UF',
            'CT_I',
            'CT_N',
            'CT_ORF1AB',
            ]

        for column in id_columns:
            if column not in df.columns.tolist():
                df[column] = ''
                print('\t\t\t - No \'%s\' column found. Please check for inconsistencies. Meanwhile, an empty \'%s\' column was added.' % (column, column))

        ## generate sample id
        df.insert(1, 'sample_id', '')
        df.insert(1, 'test_kit', 'covid_pcr')
        df.fillna('', inplace=True)


        ## assign id and deduplicate
        df, dfN = deduplicate(df, dfN, id_columns, test_name)
        #print(dfL.head())


        if df.empty:
            #print('# Returning an empty dataframe')
            return dfN

        # starting lab specific reformatting
        pathogens = {
            'SC2': [], 
            'FLUA': [],
            'FLUB': [], 
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

        for p, t in pathogens.items():
            if p != 'SC2':
                df[p + '_test_result'] = 'NT' #'Not tested'

        ## adding missing columns
        if 'Dt. Nascimento' not in df.columns.tolist():
            df['birthdate'] = ''
        df['Ct_FluA'] = ''
        df['Ct_FluB'] = ''
        df['Ct_VSR'] = ''
        df['Ct_RDRP'] = ''
        df['Ct_geneS'] = ''
        df['Ct_geneN'] = ''
        df['Ct_geneE'] = ''

        ## fix decimal values
        df['CT_N'] = df['CT_N'].str.replace(',', '.')
        df['CT_ORF1AB'] = df['CT_ORF1AB'].str.replace(',', '.')

        # [WIP] Inconcolusivos
        df['Resultado'] = df['Resultado'].apply(lambda x: 'Pos' if x.startswith('Detectado') else 'Neg' )
        return df
    else:
        # print('\t\tFile = ' + file)
        print('\t\tWARNING! Unknown file format. Check for inconsistencies.')
        # exit()
    return dfN
    # print("Done reformating")


if __name__ == '__main__':
    FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logger = logging.getLogger("HLAGYN ETL")
    # add handler to stdout
    handler = logging.StreamHandler()
    # Logger all levels
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(FORMAT)
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)

    parser = argparse.ArgumentParser(
        description="Performs diverse data processing tasks for specific HLAGyn lab cases. It seamlessly loads and combines data from multiple sources and formats into a unified dataframe. It applies renaming and correction rules to columns, generates unique identifiers, and eliminates duplicates based on prior data processing. Age information is derived from birth dates, and sex information is adjusted accordingly. The resulting dataframe is sorted by date and saved as a TSV file. Duplicate rows are also identified and saved separately for further analysis.",
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

    logger.info(f"Starting HLAGyn ETL")
    logger.info(f"Input folder: {input_folder}")
    logger.info(f"Rename file: {rename_file}")
    logger.info(f"Correction file: {correction_file}")
    logger.info(f"Cache file: {cache_file}")
    logger.info(f"Output file: {output}")

# local run
    # # path = "/Users/**/**/"
    # path = "/Users/*/respat/"
    # input_folder = path + 'data/'
    # rename_file = input_folder + 'rename_columns.xlsx'
    # correction_file = input_folder + 'fix_values.xlsx'
    # cache_file = input_folder + 'combined_cache.tsv'
    # output = input_folder + today + '_combined_data_hlagyn.tsv'

    lab_data_folder = input_folder + 'HLAGyn/'
    if not has_something_to_be_done(lab_data_folder):
        logger.info(f"No files found in {lab_data_folder}")
        if cache_file not in [np.nan, '', None]: 
            logger.info(f"Just copying {cache_file} to {output}")
            os.system(f"cp {cache_file} {output}")
        else:
            logger.info(f"No cache file found. Nothing to be done.")
        logger.info(f"Data successfully aggregated and saved in: {output}")
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
    # print("Done load tables")

    ## load cache file
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
    # print("Done create dictionary corrections")


    ## load value corrections
    dfC = load_table(correction_file)
    dfC.fillna('', inplace=True)
    dfC = dfC[dfC['lab_id'].isin(["HLAGyn", "any"])] # filter to correct data into fix_values HLAGyn
    # print("Done load corrections")

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
    # print("Load rename_columns")


    def generate_id(column_id):
        id = hashlib.sha1(str(column_id).encode('utf-8')).hexdigest()
        return id
    # print("Done generate hashlib")


    def deduplicate(dfL, dfN, id_columns, test_name):
        ## generate sample id
        dfL['unique_id'] = dfL[id_columns].astype(str).sum(axis=1)  ## combine values in rows as a long string
        dfL['sample_id'] = dfL['unique_id'].apply(lambda x: generate_id(x)[:16])  ## generate alphanumeric sample id with 16 characters

        ## prevent reprocessing of previously processed samples
        if cache_file not in [np.nan, '', None]:
            duplicates = set(dfL[dfL['sample_id'].isin(dfT['sample_id'].tolist())]['sample_id'].tolist())
            if len(duplicates) == len(set(dfL['sample_id'].tolist())):
                print('\n\t\t * ALL samples (%s) were already previously processed. All set!' % test_name)
                dfN = pd.DataFrame()  ## create empty dataframe, and populate it with reformatted data from original lab dataframe
                dfL = pd.DataFrame()

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
    # print("Done deduplication")
    


    ## Fix datatables
    print('\nFixing datatables...')
    
    def rename_columns(id, df):
        # print(df.columns.tolist())
        # print(dict_rename[id])
        if id in dict_rename:
            df = df.rename(columns=dict_rename[id])
            # print(df.columns.tolist())
        return df
    # print("Done rename")

    ## fix data points create dict from dict
    def fix_data_points(id, col_name, value):
        new_value = value
        # print(value)
        if value in dict_corrections[id][col_name]:
            new_value = dict_corrections[id][col_name][value]
            # print(str("test = ") + new_value)
        return new_value
    # print("Load dictionary for fix_values")

    ## open data files
    for sub_folder in os.listdir(input_folder):
        if sub_folder == 'HLAGyn': ## check if folder is the correct one
            id = sub_folder
            sub_folder = sub_folder + '/'

            if not os.path.isdir(input_folder + sub_folder):
                logger.error(f"Folder {input_folder + sub_folder} not found.")
                break
            
            logger.info(f"Processing DataFrame from: {id}")
            for file_i, filename in enumerate(sorted(os.listdir(input_folder + sub_folder))):
                
                if not filename.endswith(('tsv', 'csv', 'xls', 'xlsx', 'parquet')):
                    continue

                if filename[0] in ['~', '_']:
                    continue

                logger.info(f"Loading data from: {input_folder + sub_folder + filename}")
                logger.info(f"File {file_i + 1} of {len(os.listdir(input_folder + sub_folder))}")
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
                df = rename_columns(id, df) ## fix data points
                dfT = dfT.reset_index(drop=True)
                df = df.reset_index(drop=True)

                # print(df.head(2)) # only reformat
                # print(dfT.head(2)) # all labs empty without cache

                logger.info(f"Starting to fix values - {filename}")
                dict_corrections_full = {
                    #**dict_corrections['HLAGyn'] 
                    **dict_corrections['any']
                }
                df = df.replace(dict_corrections_full)
                
                ## add age from birthdate, if age is missing
                if 'birthdate' in df.columns.tolist():
                    # cast to datetime where is not np.nan, '' or None
                    df['birthdate'] = pd.to_datetime(df['birthdate'], errors='coerce').fillna('1800-01-01')
                    df['date_testing'] = pd.to_datetime(df['date_testing'], errors='coerce').fillna('2100-01-01')
                    
                    # Calculate age where it is -1
                    df['age'] = df['age'].where(
                        df['age'] > 0, 
                        ((df['birthdate'] - df['date_testing'])/np.timedelta64(1, 'Y')).round(1).astype(int)
                    )

                    # Remove AGE values < 0 and > 150 -> absurd values created by the replacement of null values
                    df['age'] = df['age'].apply(lambda x: x if x >= 0 and x <= 150 else -1) 

                logger.info(f"Finished fixing values - {filename}")
                logger.info(f"New shape: {df.shape[0]} rows and {df.shape[1]} columns")
                logger.info(f"Starting to aggregate results - {filename}")

                frames = [dfT, df]
                df2 = pd.concat(frames).reset_index(drop=True)
                dfT = df2
                logger.info(f"Finished aggregating results - {filename}")

    logger.info(f"Finished processing all files. Final shape {dfT.shape[0]} rows and {dfT.shape[1]} columns")
    dfT = dfT.reset_index(drop=True)
    dfT.fillna('', inplace=True)
    # print('Done fix tables')

    ## old place where add `Fixing data points...`

    # reformat dates and get ages
    dfT['date_testing'] = pd.to_datetime(dfT['date_testing'])

    ## create epiweek column
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

    logger.info(f"Creating epiweek column")
    dfT['epiweek'] = dfT['date_testing'].apply(lambda x: get_epiweeks(x))

    ## add gene S detection column (blank)
    dfT['geneS_detection'] = ''

    # reset index
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

    # dfT['date_testing'] = dfT['date_testing'].apply(lambda x: x.strftime('%Y-%m-%d') if x is pd.Timestamp else '')

    logger.info(f"Finding duplicates")
    ## output duplicates rows
    duplicates = dfT.duplicated().sum()
    if duplicates > 0:
        mask = dfT.duplicated(keep=False) ## find duplicates
        dfD = dfT[mask]
        output2 = input_folder + 'duplicates.tsv'
        dfD.to_csv(output2, sep='\t', index=False)
        logger.warning(f"File with {duplicates} duplicate entries saved in: {output2}")

    ## drop duplicates
    dfT = dfT.drop_duplicates(keep='last')

    ## sorting by date
    dfT = dfT.sort_values(by=['lab_id', 'test_id', 'date_testing'])

    # output combined dataframe
    dfT.to_csv(output, sep='\t', index=False)
    logger.info(f"Data successfully aggregated and saved in: {output}")
