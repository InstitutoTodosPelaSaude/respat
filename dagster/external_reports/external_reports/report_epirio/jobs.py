from dagster import define_asset_job

report_epirio_assets_job = define_asset_job(
    name="report_epirio_assets_job",
    selection=[
        "combined_for_reports",
        "report_epirio_01_filter_and_pivot",
        "report_epirio_02_group_values",
        "report_epirio_final",
        "report_epirio_export_to_tsv",
        "report_epirio_send_email"
    ]
)