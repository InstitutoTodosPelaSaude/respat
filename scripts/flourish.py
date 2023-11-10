## -*- coding: utf-8 -*-

## Created by: Bragatte
## Email: marcelo.bragatte@itps.org.br
## Release date: 2023-09-01 | Updated: 2023-11-08

import argparse
import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta


def generate_bar_posneg():
    bar_posneg = pd.read_csv("barplot/combined_matrix_country_posneg_allpat_weeks.tsv", sep='\t')
    bar_posneg.drop('country', axis=1, inplace=True)
    bar_posneg_melted = pd.melt(bar_posneg, id_vars='test_result', var_name='semana epidemiológica', value_name='testes')
    
    bar_posneg_positivos = bar_posneg_melted[bar_posneg_melted['test_result'] == 'Pos'].reset_index(drop=True)
    bar_posneg_negativos = bar_posneg_melted[bar_posneg_melted['test_result'] == 'Neg'].reset_index(drop=True)
    
    bar_posneg_combined = pd.concat([bar_posneg_positivos['semana epidemiológica'], bar_posneg_positivos['testes'], bar_posneg_negativos['testes']], axis=1)
    bar_posneg_combined.columns = ['semana epidemiológica', 'Positivos', 'Negativos']
    
    bar_posneg_combined.to_excel('barplot/bar_posneg.xlsx', index=False)
    bar_posneg_combined.to_csv('barplot/bar_posneg.csv', index=False)

def generate_bar_panels():
    bars = pd.read_csv("barplot/combined_matrix_country_posneg_panel_weeks.tsv", sep='\t')
    bars.drop('country', axis=1, inplace=True)
    bars_melted = pd.melt(bars, id_vars=['pathogen', 'test_result'], var_name='semana epidemiológica', value_name='casos').reset_index(drop=True)
    bars_pos = bars_melted[bars_melted['test_result'] == 'Pos'].reset_index(drop=True)
    bars_pos['casos'] = bars_pos['casos'].astype(int)
    
    bars_piv = bars_pos.pivot_table(index='semana epidemiológica', columns='pathogen', values='casos', aggfunc='sum').reset_index()
    
    bars = bars_piv.reindex(columns=['semana epidemiológica', 'RINO', 'ENTERO', 'META', 'PARA', 'BOCA', 'COVS','ADENO', 'BAC','FLUA', 'FLUB', 'SC2', 'VSR'])
    bars = bars.rename(columns={'RINO': 'Rinovírus', 'ENTERO': 'Enterovírus', 'META': 'Metapneumovírus', 'PARA': 'Vírus Parainfluenza', 'BOCA': 'Bocavírus', 'COVS': 'Coronavírus sazonais', 'ADENO': 'Adenovírus', 'BAC': 'Bactérias', 'FLUA': 'Influenza A',  'FLUB': 'Influenza B', 'SC2':'SARS-CoV-2', 'VSR':'Vírus Sincicial Respiratório'})
    
    bars.to_excel('barplot/bar_panels.xlsx', index=False)
    bars.to_csv('barplot/bar_panels.csv', index=False)    

def generate_line_plots():
    lines = pd.read_csv("lineplot/combined_matrix_country_posrate_full_weeks.tsv", sep='\t')
    lines.drop(['country', 'test_result'], axis=1, inplace=True)
    
    lines_melted = pd.melt(lines, id_vars=['pathogen'], var_name='semana epidemiológica', value_name='casos').reset_index(drop=True)
    lines_piv = lines_melted.pivot_table(index='semana epidemiológica', columns='pathogen', values='casos', aggfunc='sum').reset_index()
    
    lines_piv = lines_piv.reindex(columns=['semana epidemiológica', 'RINO', 'ENTERO', 'META', 'PARA', 'BOCA', 'COVS', 'ADENO', 'BAC', 'FLUA', 'FLUB', 'SC2', 'VSR'])
    lines_piv = lines_piv.rename(columns={'RINO': 'Rinovírus', 'ENTERO': 'Enterovírus', 'META': 'Metapneumovírus', 'PARA': 'Vírus Parainfluenza', 'BOCA': 'Bocavírus', 'COVS': 'Coronavírus sazonais', 'ADENO': 'Adenovírus', 'BAC': 'Bactérias', 'FLUA': 'Influenza A', 'FLUB': 'Influenza B', 'SC2': 'SARS-CoV-2', 'VSR': 'Vírus Sincicial Respiratório'})
    
    lines_piv.to_excel('lineplot/line_full.xlsx', index=False)
    
def generate_heatmap_positivos():
    targets = [('VSR', 'Virus_Sincicial_Resp'), ('FLUA', 'Influenza_A'), ('SC2', 'SARS-CoV-2')]
    
    for target, rename_target in targets:
        heat = pd.read_csv(f"heatmap/matrix_agegroups_weeks_{target}_posrate.tsv", sep='\t')
        heat_cleaned = heat.drop('country', axis=1).dropna(axis=1)

        heat_cleaned = heat_cleaned.rename(columns={'age_group': 'faixas etárias'})
        heat_cleaned = heat_cleaned.loc[~heat_cleaned[f'{target}_test_result'].str.contains('Não Detectado|Not tested')]
        heat_cleaned['faixas etárias'] = heat_cleaned['faixas etárias'].replace({'0-4': '00-04', '4-9': '05-09', '9-19': '10-19', '19-29': '20-29', '29-39': '30-39', '39-49': '40-49', '49-59': '50-59', '59-69': '60-69', '69-79': '70-79'})

        heat_melted = pd.melt(heat_cleaned, id_vars=[f'{target}_test_result', 'faixas etárias'], var_name='semana epidemiológica', value_name='percentual')
        heat_melted['percentual'] = heat_melted['percentual'].apply(lambda x: f'{round(x * 100, 2)}%')
        heat_melted_positivos = heat_melted[heat_melted[f'{target}_test_result'] == 'Pos'].reset_index(drop=True)

        heat_melted_positivos.to_excel(f'heatmap/heatmap_{rename_target}.xlsx', index=False)
        heat_melted_positivos.to_csv(f'heatmap/heatmap_{rename_target}.csv', index=False)        
        

def generate_heatmap_estados():
    heat_ufs = pd.read_csv("heatmap/combined_matrix_state_posrate_full_weeks.tsv", sep='\t')
    melted_ufs = heat_ufs.melt(id_vars=['state', 'pathogen', 'test_result', 'state_code', 'country'], 
                               var_name='semana epidemiológica', 
                               value_name='percentual')
    melted_ufs['percentual'] = melted_ufs['percentual'].apply(lambda x: f'{round(x * 100, 2)}')
    heatmap_sc2 = melted_ufs[melted_ufs['pathogen'] == 'SC2']
    columns_to_remove = ['state', 'test_result', 'country', 'pathogen']
    heatmap_uf_sc2 = heatmap_sc2.drop(columns=columns_to_remove)
    heatmap_uf_sc2 = heatmap_uf_sc2.rename(columns={'state_code': 'UF'})
    heatmap_uf_sc2.replace('nan', np.nan, inplace=True)
    
    porcentagem = heatmap_uf_sc2.groupby('UF')['percentual'].apply(lambda x: (x.notna().sum() / len(x)) * 100)
    estados_selecionados = porcentagem[porcentagem > 70].index
    heatmap_ufs = heatmap_uf_sc2[heatmap_uf_sc2['UF'].isin(estados_selecionados)].dropna()

    regioes = {
        'Norte': ['AC', 'AM', 'AP', 'PA', 'RO', 'RR', 'TO'],
        'Nordeste': ['AL', 'BA', 'CE', 'MA', 'PB', 'PE', 'PI', 'RN', 'SE'],
        'Sudeste': ['ES', 'MG', 'RJ', 'SP'],
        'Sul': ['PR', 'RS', 'SC'],
        'Centro-Oeste': ['DF', 'GO', 'MT', 'MS']
    }
    heatmap_ufs['Regiao'] = heatmap_ufs['UF'].map({estado: regiao for regiao, estados in regioes.items() for estado in estados})
    heatmap_ufs = heatmap_ufs.sort_values(['Regiao', 'UF'])
    
    heatmap_ufs.to_excel('heatmap/heatmap_states.xlsx', index=False)
    heatmap_ufs.to_csv('heatmap/heatmap_states.csv', index=False)

    
def generate_pyramid_totaltestpanel(date_filter):
    pyr_t = pd.read_csv("pyramid/combined_matrix_agegroup.tsv", sep='\t')
    column_mapping = {'0-4': '00-04', '4-9': '05-09', '9-19': '10-19', '19-29': '20-29', '29-39': '30-39', '39-49': '40-49', '49-59': '50-59', '59-69': '60-69', '69-79': '70-79'}
    pyr_t = pyr_t.rename(columns=column_mapping)
    
    pyr_t_melt = pyr_t.melt(id_vars=['name', 'pathogen', 'test_result', 'epiweek'], var_name='faixas_etárias', value_name='casos')
    pyr_pos = pyr_t_melt[pyr_t_melt['test_result'] == 'Pos'].reset_index(drop=True)
    pyr_piv = pyr_pos.pivot_table(index=('epiweek','faixas_etárias'), columns='pathogen', values='casos', aggfunc='sum').reset_index()

    ### CHANGE WEEKS HERE IF MANUALLY
    # datas_filtradas = ['2023-08-05', '2023-08-12', '2023-08-19', '2023-08-26']
    pyr_dates = pyr_piv[pyr_piv['epiweek'].isin(date_filter)]
    
    pyr_ages = pyr_dates.reindex(columns=['epiweek', 'faixas_etárias', 'RINO', 'ENTERO', 'META', 'PARA', 'BOCA', 'COVS','ADENO', 'BAC','FLUA', 'FLUB', 'SC2', 'VSR'])
    pyr_ages = pyr_ages.rename(columns={'epiweek':'semana_epidemiológica','RINO': 'Rinovírus', 'ENTERO': 'Enterovírus', 'META': 'Metapneumovírus', 'PARA': 'Vírus Parainfluenza', 'BOCA': 'Bocavírus', 'COVS': 'Coronavírus sazonais', 'ADENO': 'Adenovírus', 'BAC': 'Bactérias', 'FLUA': 'Influenza A',  'FLUB': 'Influenza B', 'SC2':'SARS-CoV-2', 'VSR':'Vírus Sincicial Respiratório'})
    
    pyr_ages.to_excel('pyramid/pyr_agegroups.xlsx', index=False)
    pyr_ages.to_csv('pyramid/pyr_agegroups.csv', index=False)

def flourish_plots(path_flourish, end_date):
    ## Change work directory for path_flourish
    os.chdir(path_flourish)
    
    ## Extracting end_date from argparse and converting to datetime object
    end_date_obj = datetime.strptime(args.end_date, '%Y-%m-%d')

    ## Calculate the four weeks leading up to end_date
    date_filter = [(end_date_obj - timedelta(days=7*i)).strftime('%Y-%m-%d') for i in range(4)]
    
    ## Bar plot section
    generate_bar_posneg()
    generate_bar_panels()
    
    ## Lineplot section
    generate_line_plots()

    ## Heatmap section
    generate_heatmap_positivos()
    generate_heatmap_estados()
    
    ## Pyramid section
    generate_pyramid_totaltestpanel(date_filter)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Script to generate Flourish plots from data')
    parser.add_argument('--path_flourish', help='Path to the flourish folder inside figures dir', required=False)
    parser.add_argument('--end_date', help='End date for the analysis', required=True)
    
    args = parser.parse_args()
    
    flourish_plots(args.path_flourish, args.end_date)
