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



import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.simplefilter(action='ignore', category=pd.errors.PerformanceWarning)

pd.set_option('display.max_columns', 500)
pd.options.mode.chained_assignment = None

today = time.strftime('%Y-%m-%d', time.gmtime()) ## for snakefile

if __name__ == '__main__':
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
    ## print("Done load tables")

    ## load cache file
    if cache_file not in [np.nan, '', None]:
        dfT = load_table(cache_file)
        dfT.fillna('', inplace=True)
    else:
        dfT = pd.DataFrame()

    ## load renaming patterns
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

    ## load value corrections
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
                dfN = pd.DataFrame()  # create empty dataframe, and populate it with reformatted data from original lab dataframe
                dfL = pd.DataFrame()

                return dfN, dfL
            else:
                print('\n\t\t * A total of %s out of %s samples (%s) were already previously processed.' % (str(len(duplicates)), str(len(set(dfL['sample_id'].tolist()))), test_name))
                new_samples = len(set(dfL['sample_id'].tolist())) - len(duplicates)
                print('\t\t\t - Processing %s new samples...' % (str(new_samples)))
                dfL = dfL[~dfL['sample_id'].isin(dfT['sample_id'].tolist())]  # remove duplicates
        else:
            new_samples = len(dfL['sample_id'].tolist())
            print('\n\t\t\t - Processing %s new samples (%s)...' % (str(new_samples), test_name))

        return dfL, dfN
    # print("Done deduplication")
    


    ## Fix datatables
    def fix_datatable(dfL,file):
        PARAMETERS_21_TESTS = {
            'PARA1',
            'PARA2',
            'PARA3',
            'PARA4',
            'BORDETELLAP',
            'VSINCICIAL',
            'CPNEUMONIAE',
            'ADEN',
            'CORON',
            'CORHKU',
            'CORNL',
            'CORC',
            'HUMANMET',
            'HUMANRH',
            'INFLUEH',
            'INFLUEN',
            'INFLUENZ',
            'INFLUEB',
            'MYCOPAIN',
            'PAINSARS',
            'RSPAIN',
        }

        dfN = dfL
        if 'OS' in dfL.columns.tolist(): # and '' in dfL['Codigo'].tolist(): #column with unique row data
            test_name = "covid"

            ## define columns dtypes to reduce the use of memory
            dfL["OS"] = dfL["OS"].astype('str')
            dfL["Código Posto"] = dfL["Código Posto"].astype('int16')
            dfL["Estado"] = dfL["Estado"].astype('str')
            dfL["Municipio"] = dfL["Municipio"].astype('str')
            dfL["DataAtendimento"] = pd.to_datetime(dfL["DataAtendimento"])
            dfL["DataNascimento"] = pd.to_datetime(dfL["DataNascimento"])
            dfL["Sexo"] = dfL["Sexo"].astype('str')
            dfL["Descricao"] = dfL["Descricao"].astype('str')
            dfL["Parametro"] = dfL["Parametro"].astype('str')
            dfL["Resultado"] = dfL["Resultado"].astype('str')
            dfL["DataAssinatura"] = pd.to_datetime(dfL["DataAssinatura"])
            # print(dfL.dtypes)

            ## add sample_id and test_kit
            dfL.insert(1, 'sample_id', '')
            dfL.insert(1, 'test_kit', '')

            # Test Kit Covid
            dfL["test_kit"] = df["Parametro"].apply(lambda x: "covid_antigen" if x == "COVIDECO" else "covid_pcr") # separate by rows in column test_kit
            
            # Test Kit 21 -> Painel Molecular
            dfL["test_kit"] = dfL["test_kit"].apply(lambda x: "test_21" if x in PARAMETERS_21_TESTS else x)

            dfL.fillna('', inplace=True)

            id_columns = [
                'OS',
                'Estado',
                'Municipio',
                'DataAtendimento',
                'Sexo',
                'Descricao',
                'Resultado',
                ] 

            for column in id_columns:
                if column not in dfL.columns.tolist():
                    dfL[column] = ''
                    print('\t\t\t - No \'%s\' column found. Please check for inconsistencies. Meanwhile, an empty \'%s\' column was added.' % (column, column))

            ## adding missing columns
            if 'DataNascimento' not in dfL.columns.tolist():
                dfL['birthdate'] = ''
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

            if dfL.empty:
                #print('# Returning an empty dataframe')
                return dfN

            ## starting reformatting process

            PATHOGENS_PARAMETERS = {
                'SC2': {'NALVO', 'PCRSALIV', 'COVIDECO', 'TMR19RES1', 'NALVOSSA',
                        'NALVOCTL', 'RDRPALVOCTL', 'RDRPALVO', 'RDRPCI', 'NALVOCI',
                        'NALVOCQ'},
                'FLUA':{},
                'FLUB':{},
                'VSR':{},
                'META':{},
                'RINO':{},
                'PARA':{'PARA1','PARA2','PARA3','PARA4'},
                'ADENO':{},
                'BOCA':{},
                'COVS':{},
                'ENTERO':{},
                'BAC':{}
            }

            # python scripts/reformat_sabin.py --datadir data --rename data/rename_columns.xlsx --correction data/fix_values.xlsx --output combined_hla_test.tsv

            for pathogen, parameter_list in PATHOGENS_PARAMETERS.items():
                test_result = pathogen + '_test_result'
                dfL[test_result] = dfL.apply(
                    lambda x: 'NT' if x['Parametro'] not in parameter_list else x['Resultado'], 
                    axis=1
                )
            dfN = dfL

        else:
            print('\t\tWARNING! Unknown file format. Check for inconsistencies.')
        
        return dfN

    def rename_columns(id, df):
        if id in dict_rename:
            df = df.rename(columns=dict_rename[id])
        return df
    
    # print("Load dictionary for fix_values")

    ## open data files
    for element in os.listdir(input_folder):
        if not element.startswith('_'):
            if element == 'SABIN': # check if folder is the correct one
                id = element
                element = element + '/'
                if os.path.isdir(input_folder + element) == True:
                    print('\n# Processing datatables from: ' + id)
                    for filename in sorted(os.listdir(input_folder + element)):
                        if filename.split('.')[-1] in ['tsv', 'csv', 'xls', 'xlsx', 'parquet'] and filename[0] not in ['~', '_']:
                            print('\n\t- File: ' + filename)
                            df = load_table(input_folder + element + filename)
                            df.fillna('', inplace=True)
                            df.reset_index(drop=True)

                            df = fix_datatable(df, filename) ## reformat datatable
                            if df.empty:
                                # print('##### Nothing to be done')
                                continue

                            df.insert(0, 'lab_id', id)
                            df = rename_columns(id, df)
                            dfT = dfT.reset_index(drop=True)
                            df = df.reset_index(drop=True)  
                            
                            print('\n# Fixing data points...')
                            dict_corrections_full = {**dict_corrections['SABIN'], **dict_corrections['any']}
                            df = df.replace(dict_corrections_full) 

                            # Replace BITHDATE null values (np.nan, None, '') with 1800-01-01
                            df['birthdate'] = df['birthdate'].replace([np.nan, None, ''], '1700-01-01')
                            # Replace DATE_TESTING null values (np.nan, None, '') with 2200-01-01
                            df['date_testing'] = df['date_testing'].replace([np.nan, None, ''], '2300-01-01')

                            # Calculate AGE from BIRTHDATE and DATE_TESTING
                            df['age'] = (pd.to_datetime(df['date_testing']) - pd.to_datetime(df['birthdate'])) / np.timedelta64(1, 'Y')
                            df['age'] = df['age'].apply(lambda x: np.round(x, 1)) # round to 1 decimal
                            df['age'] = df['age'].apply(lambda x: int(x))
                            # Remove AGE values < 0 and > 150
                            df['age'] = df['age'].apply(lambda x: x if x >= 0 and x <= 150 else -1)


                            ## fix sex information
                            df['sex'] = df['sex'].apply(lambda x: x[0] if x != '' else x)
                            
                            frames = [dfT, df]
                            df2 = pd.concat(frames).reset_index(drop=True)
                            dfT = df2


    dfT = dfT.reset_index(drop=True)
    dfT.fillna('', inplace=True)
    # print('Done fix tables')

    ## old place where add `Fixing data points...`

    ## reformat dates and get ages
    dfT['date_testing'] = pd.to_datetime(dfT['date_testing'])

    ## create epiweek column
    def get_epiweeks(date):
        try:
            date = pd.to_datetime(date)
            epiweek = str(Week.fromdate(date, system="cdc")) ## get epiweeks
            year, week = epiweek[:4], epiweek[-2:]
            epiweek = str(Week(int(year), int(week)).enddate())
            # epiweek = str(Week.fromdate(date, system="cdc"))  ## get epiweeks
            # epiweek = epiweek[:4] + '_' + 'EW' + epiweek[-2:]
        except:
            epiweek = ''
        return epiweek

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
            print('Adding missing column: %s' % col)
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


    # time controller for optimization of functions `def`
    ## example with tqdm for def
    # start = time.time()
    # for load_table in tqdm(range(1), desc='Execution Time'):
    #     load_table
    # end = time.time()
    # print("Execution time for load_table: ", end - start)

    ## output combined dataframe
    dfT.to_csv(output, sep='\t', index=False)
    print('\nData successfully aggregated and saved in:\n%s\n' % output)