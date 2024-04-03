from dagster import AssetExecutionContext
from dagster import (
    AssetExecutionContext,
    asset,
    MaterializeResult, 
    MetadataValue
)
from dagster_dbt import (
    DbtCliResource, 
    dbt_assets,
    DagsterDbtTranslator,
    DagsterDbtTranslatorSettings,
    get_asset_key_for_model
)
from .constants import dbt_manifest_path
from textwrap import dedent
import pandas as pd
import os
import pathlib
from sqlalchemy import create_engine
from dotenv import load_dotenv


dagster_dbt_translator = DagsterDbtTranslator(
    settings=DagsterDbtTranslatorSettings(enable_asset_checks=True)
)

load_dotenv()
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_SCHEMA = os.getenv('DB_SCHEMA')

ROOT_PATH = pathlib.Path(__file__).parent.parent.parent.parent.absolute()
SAVE_PATH = ROOT_PATH / "data" / "matrices"

@dbt_assets(
    manifest=dbt_manifest_path,
    select='matrices',
    dagster_dbt_translator=dagster_dbt_translator
)
def respiratorios_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()


def generate_matrix(name, aggregate_columns, pivot_column, metrics, filters):
    
    engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')

    table_columns = [
        'pathogen',
        'lab_id',
        'test_kit',
        'state_code',
        'country',
        'epiweek_enddate',
        'age_group',
        'state',
    ]

    all_columns = aggregate_columns + [pivot_column]

    null_columns = [column for column in table_columns if column not in all_columns]
    all_columns += ['result', 'metric']

    all_columns = list(set(all_columns))
    null_columns = list(set(null_columns))
    metrics = list(set(metrics))

    metrics_tuple = ", ".join([f"'{metric}'" for metric in metrics])

    query = f"""
        SELECT
            {', '.join(all_columns)}
        FROM
            {DB_SCHEMA}.matrices_03_unpivot_metrics
        WHERE
            metric IN ({metrics_tuple})
            AND {' AND '.join(
                [f"{column} IS NULL" for column in null_columns]
                )
                if len(null_columns) > 0 
                else '1=1'
            }
            AND {' AND '.join([f"{column} IS NOT NULL" for column in all_columns if column not in null_columns])}
    """

    if len(filters) > 0:
        query += f" AND {' AND '.join(filters)}"

    # save query to txt
    # with open(SAVE_PATH / f'{name}.txt', 'w') as f:
    #    f.write(query)

    df = pd.read_sql(query, engine)

    # if posrate not in metrics, turn all values to int
    if 'posrate' not in metrics:
        df['result'] = df['result'].astype(int)

    pivot_df = df.pivot(
        index=aggregate_columns+['metric'],
        columns=pivot_column,
        values='result'
    ).reset_index()

    pivot_df.columns.name = None

    # fill NA with 0
    pivot_df = pivot_df.fillna(0)

    pivot_df.to_csv(SAVE_PATH / name, sep='\t', index=False)


@asset(
    compute_kind="python",
    deps=[get_asset_key_for_model([respiratorios_dbt_assets], "matrices_03_unpivot_metrics")]
)
def generate_matrices(context):
    """
    Generate matrices from the data
    """
    matrices = [
        (
            'combined_matrix_country_posneg_allpat_weeks.tsv', 
            [
             'country',
            ],
            'epiweek_enddate',
            ['Pos', 'Neg'],
            [],
        ),
        (
            'combined_matrix_country_posneg_full_weeks.tsv', 
            [
             'country', 'pathogen',
            ],
            'epiweek_enddate',
            ['Pos', 'Neg'],
            [],
        ),
        (
            'combined_matrix_country_posneg_panel_weeks.tsv', 
            [
             'country', 'pathogen',
            ],
            'epiweek_enddate',
            ['Pos', 'Neg'],
            [],
        ),
        (
            'matrix_agegroups_weeks_FLUA_posrate.tsv', 
            [
             'pathogen', 'age_group', 'country'
            ],
            'epiweek_enddate',
            ['posrate'],
            ["pathogen='FLUA'"],
        ),
        (
            'matrix_agegroups_weeks_FLUB_posrate.tsv', 
            [
             'pathogen', 'age_group', 'country'
            ],
            'epiweek_enddate',
            ['posrate'],
            ["pathogen='FLUB'"],
        ),
        (
            'matrix_agegroups_weeks_SC2_posrate.tsv', 
            [
             'pathogen', 'age_group', 'country'
            ],
            'epiweek_enddate',
            ['posrate'],
            ["pathogen='SC2'"],
        ),
        (
            'matrix_agegroups_weeks_VSR_posrate.tsv', 
            [
             'pathogen', 'age_group', 'country'
            ],
            'epiweek_enddate',
            ['posrate'],
            ["pathogen='VSR'"],
        ),
        (
            'combined_matrix_country_posrate_full_weeks.tsv', 
            [
             'country', 'pathogen',
            ],
            'epiweek_enddate',
            ['posrate'],
            [],
        ),
        (
            'combined_matrix_agegroup.tsv', 
            [
             'country', 'pathogen', 'epiweek_enddate'
            ],
            'age_group',
            ['Pos', 'Neg'],
            [],
        ),
    ]

    for matrix in matrices:
        generate_matrix(*matrix)

    return MaterializeResult(
        metadata={
            "info": MetadataValue.md(dedent(f"""
            # Matrices generated

            Last updated: {pd.Timestamp.now() - pd.Timedelta(hours=3)}
            """))
        }
    )

@asset(
    compute_kind="python",
    deps=[generate_matrices]
)
def adapt_and_rename_matrices(context):
    """
    Adapt and rename matrices.
    The objective is to make the matrices compatible with the plotting scripts.
    """
    
    rename_column_metric_for_test_result = [
        'combined_matrix_country_posneg_allpat_weeks.tsv',
        'combined_matrix_country_posneg_full_weeks.tsv',
        'combined_matrix_country_posneg_panel_weeks.tsv',
        'combined_matrix_country_posrate_full_weeks.tsv',
        'combined_matrix_agegroup.tsv'
    ]

    rename_column_metric_for_pathogen_name = [
        'matrix_agegroups_weeks_FLUA_posrate.tsv',
        'matrix_agegroups_weeks_FLUB_posrate.tsv',
        'matrix_agegroups_weeks_SC2_posrate.tsv',
        'matrix_agegroups_weeks_VSR_posrate.tsv',
    ]
    
    for matrix in rename_column_metric_for_test_result:
        df = pd.read_csv(SAVE_PATH / matrix, sep='\t')
        df = df.rename(columns={'metric': 'test_result'})

        # if 'posrate' is in the name
        # map all the 'posrate' values in the 'test_result'' column to 'Pos'
        if 'posrate' in matrix:
            df['test_result'] = df['test_result'].map({'posrate': 'Pos'})

        df.to_csv(SAVE_PATH / matrix, sep='\t', index=False)

    for matrix in rename_column_metric_for_pathogen_name:
        df = pd.read_csv(SAVE_PATH / matrix, sep='\t')
        pathogen_name = matrix.split('_')[3]

        # if 'posrate' is in the name
        # map all the 'posrate' values in the 'test_result'' column to 'Pos'
        if 'posrate' in matrix:
            df['metric'] = df['metric'].map({'posrate': 'Pos'})

        # if patogen name is in columns, drop it
        if 'pathogen' in df.columns:
            df = df.drop(columns='pathogen')
    
        df = df.rename(columns={'metric': pathogen_name + '_test_result'})

        df.to_csv(SAVE_PATH / matrix, sep='\t', index=False)


    return MaterializeResult(
        metadata={
            "info": MetadataValue.md(dedent(f"""
            # Matrices Renamed

            Last updated: {pd.Timestamp.now() - pd.Timedelta(hours=3)}
            """))
        }
    )



