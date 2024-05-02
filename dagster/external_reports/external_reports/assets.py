from dagster import AssetExecutionContext
from dagster_dbt import (
    DbtCliResource, 
    dbt_assets,
    DagsterDbtTranslatorSettings,
    DagsterDbtTranslator
)

from .constants import dbt_manifest_path

dagster_dbt_translator = DagsterDbtTranslator(
    settings=DagsterDbtTranslatorSettings(enable_asset_checks=True)
)

@dbt_assets(
    manifest=dbt_manifest_path,
    select='external_reports',
    dagster_dbt_translator=dagster_dbt_translator
)
def all_external_reports(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()