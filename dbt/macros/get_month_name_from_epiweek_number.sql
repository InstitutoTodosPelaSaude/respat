{% macro get_month_name_from_epiweek_number(week_number) %}
    CASE
        WHEN {{week_number}} >= 1 and {{week_number}} < 5 THEN 'Jan'
        WHEN {{week_number}} >= 5 and {{week_number}} < 9 THEN 'Fev'
        WHEN {{week_number}} >= 9 and {{week_number}} < 13 THEN 'Mar'
        WHEN {{week_number}} >= 13 and {{week_number}} < 18 THEN 'Abr'
        WHEN {{week_number}} >= 18 and {{week_number}} < 22 THEN 'Mai'
        WHEN {{week_number}} >= 22 and {{week_number}} < 26 THEN 'Jun'
        WHEN {{week_number}} >= 26 and {{week_number}} < 31 THEN 'Jul'
        WHEN {{week_number}} >= 31 and {{week_number}} < 35 THEN 'Ago'
        WHEN {{week_number}} >= 35 and {{week_number}} < 39 THEN 'Set'
        WHEN {{week_number}} >= 39 and {{week_number}} < 44 THEN 'Out'
        WHEN {{week_number}} >= 44 and {{week_number}} < 48 THEN 'Nov'
        WHEN {{week_number}} >= 48 THEN 'Dez'
    END
{% endmacro %}