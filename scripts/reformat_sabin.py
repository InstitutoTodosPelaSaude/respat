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
    """Replace the date for its epidemiological week

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


def deduplicate(dfL, dfN, id_columns, cache_file, dfT, test_name='<TEST_NAME>'):
    """
    Remove duplicates from a dataframe based on a list of columns.

    Args:
        dfL (pandas DataFrame): Dataframe to be deduplicated
        dfN (pandas DataFrame): [WIP]
        id_columns (list of str): List of columns to be used as unique identifiers
        test_name (str, optional): Test name used to print messages

    Returns:
        tuple of DataFrames: Dataframe containing deduplicated data and dataframe containing previously processed data
    """

    print('\n\t\t * Deduplicating %s data...' % test_name)

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


def fix_datatable(dfL,file=None):
    """
    Fixes dataframe errors. Adds pathogen _test_result columns, test_kit columns, and deduplicates entries.

    Args:
        dfL (pandas dataframe): dataframe to be fixed
        file (str, optional): file path. Defaults to None.

    Returns:
        pandas dataframe: fixed dataframe
    """        

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
    if 'OS' not in dfL.columns.tolist():
        print('\t\tWARNING! Unknown file format. Check for inconsistencies.')
        return dfN
    
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

    ## add sample_id and test_kit
    dfL.insert(1, 'sample_id', '')
    dfL.insert(1, 'test_kit', '')

    # Test Kit Covid
    # Test Kit 21 -> Painel Molecular
    dfL["test_kit"] = df["Parametro"].apply(
        lambda x: 
            "covid_antigen" 
            if x == "COVIDECO" 
            else "covid_pcr" 
            if x not in PARAMETERS_21_TESTS 
            else "test_21"
    )

    test_name = 'test_21' if 'test_21' in dfL['test_kit'].tolist() else 'covid'
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
    dfL, dfN = deduplicate(
        dfL, dfN, id_columns, 
        cache_file, dfT, 
        test_name=test_name
    )

    if dfL.empty:
        return dfN


    PATHOGENS_PARAMETERS = {
        'SC2': {
            # All the parameters from the COVID-exclusive SABIN file
            'NALVO', 'PCRSALIV', 'COVIDECO', 'TMR19RES1', 'NALVOSSA',
            'NALVOCTL', 'RDRPALVOCTL', 'RDRPALVO', 'RDRPCI', 'NALVOCI',
            'NALVOCQ',
            
            'PAINSARS', # SARS-COV2
        },
        'FLUA':{
            'INFLUEH', # INFLUENZA A (H3N2)
            'INFLUEN', # INFLUENZA A (H1N1)
            'INFLUENZ' # INFLUENZA A (H1N1 - 2009)
        },
        'FLUB':{
            'INFLUEB' # INFLUENZA B
        },
        'VSR':{
            'VSINCICIAL' # VÍRUS SINCICIAL RESPIRATÓRIO
        },
        'META':{
            'HUMANMET',  # METAPNEUMOVÍRUS HUMANO
        },
        'RINO':{
            'HUMANRH',   # RHINOVÍRUS HUMANO
        },
        'PARA':{
            'PARA1','PARA2','PARA3','PARA4'
        },
        'ADENO':{
            'ADEN', # ADENOVIRUS
        },
        'COVS':{
            'CORON',       # CORONAVÍRUS 229E (?)
            'CORHKU',      # CORONAVÍRUS HKU1
            'CORNL',       # CORONAVÍRUS NL63
            'CORC',        # CORONAVÍRUS OC43
        },
        'BAC':{
            'CPNEUMONIAE', # CLAMYDOPHILA PNEUMONIA
            'MYCOPAIN',    # MYCOPLASMA PNEUMONIAE
            'BORDETELLAP', # BORDETELLA PERTUSSIS
            'RSPAIN',      # BORDETELLA PARAPEERTUSSIS (IS1001)
        },
        'BOCA':{},
        'ENTERO':{},
    }
    

    for pathogen, parameter_list in PATHOGENS_PARAMETERS.items():
        test_result = pathogen + '_test_result'
        dfL[test_result] = dfL.apply(
            lambda x: 'NT' if x['Parametro'] not in parameter_list else x['Resultado'], 
            axis=1
        )
    
    dfN = dfL
    return dfN


def rename_columns(dict_rename, df):
    """Rename columns based on a dictionary of rules.

    Args:
        id (str): Key to select the dictionary of rules.
        df (pandas DataFrame): Dataframe to be renamed.

    Returns:
        pandas DataFrame: Renamed dataframe.
    """    
    df = df.rename(columns=dict_rename)
    return df


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

    ## load cache file
    if cache_file not in [np.nan, '', None]:
        print(f'\t\t - Loading cache file... {cache_file}')

        dfT = load_table(cache_file)
        dfT.fillna('', inplace=True)
    else:
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

    ## open data files
    for sub_folder in os.listdir(input_folder):
        if sub_folder == 'SABIN': # check if folder is the correct one
            id = sub_folder
            sub_folder = sub_folder + '/'

            if not os.path.isdir(input_folder + sub_folder):
                print('\n# ERROR! Folder not found: ' + input_folder + sub_folder)
                break
            
            print('\n# Processing datatables from: ' + id)

            for filename in sorted(os.listdir(input_folder + sub_folder)):
                
                if not filename.endswith( ('.tsv', '.csv', '.xls', '.xlsx', '.parquet') ):
                    continue
                if filename.startswith( ('~', '_') ):
                    continue

                print(f'\n\t- File: {filename}' )

                df_path = input_folder + sub_folder + filename
                df = load_table(df_path)
                df.fillna('', inplace=True)
                df.reset_index(drop=True)

                df = fix_datatable(df, filename)
                if df.empty:
                    print( f"\t\tWARNING! Empty file {df_path}. Check for inconsistencies.")
                    continue

                df.insert(0, 'lab_id', id)
                df = rename_columns(dict_rename[id], df)
                dfT = dfT.reset_index(drop=True)
                df = df.reset_index(drop=True)  
                
                print('\n# Fixing data points...')

                # Joining the generic corrections with the lab-specific ones
                dict_corrections_full = {**dict_corrections['SABIN'], **dict_corrections['any']}
                df = df.replace(dict_corrections_full) 


                # Calculate AGE from BIRTHDATE and DATE_TESTING
                # Replacing null values with 1700-01-01 and 2300-01-01 to avoid errors

                df['birthdate'] = df['birthdate'].replace([np.nan, None, ''], '1700-01-01')
                df['date_testing'] = df['date_testing'].replace([np.nan, None, ''], '2300-01-01')
                df['age'] = (pd.to_datetime(df['date_testing']) - pd.to_datetime(df['birthdate'])) / np.timedelta64(1, 'Y')
                df['age'] = df['age'].apply(lambda x: np.round(x, 1)) # round to 1 decimal
                df['age'] = df['age'].apply(lambda x: int(x))

                # Remove AGE values < 0 and > 150 -> absurd values created by the replacement of null values
                df['age'] = df['age'].apply(lambda x: x if x >= 0 and x <= 150 else -1)


                ## fix sex information
                df['sex'] = df['sex'].apply(lambda x: x[0] if x != '' else x)
                
                frames = [dfT, df]
                df2 = pd.concat(frames).reset_index(drop=True)
                dfT = df2

                print('\t\t- Finished processing file: ' + filename)
                    


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
            print('WARNING! Column %s not found in the table. Adding it with empty values.' % col)
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
        print('\nWARNING!\nFile with %s duplicate entries saved in:\n%s' % (str(duplicates), output2))

    dfT = dfT.drop_duplicates(keep='last')
    dfT = dfT.sort_values(by=['lab_id', 'test_id', 'date_testing'])

    dfT.to_csv(output, sep='\t', index=False)
    print('\nData successfully aggregated and saved in:\n%s\n' % output)