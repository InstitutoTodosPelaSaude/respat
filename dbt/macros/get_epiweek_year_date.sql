{% macro get_epiweek_year_date(date_expr) -%}
    -- SINAN rule: assign the epidemiological year to the year that contains most days of the week (use the week's Wednesday).
    EXTRACT(
        YEAR
        FROM (
            (DATE_TRUNC('week', {{ date_expr }} + INTERVAL '1 day') - INTERVAL '1 day')  -- Sunday
            + INTERVAL '3 day'                                                           -- Wednesday
        )
    )
{%- endmacro %}