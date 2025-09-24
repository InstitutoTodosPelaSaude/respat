{% test accepted_pathogen_detail(model) %}

    SELECT
        pathogen_name, pathogen_detail
    FROM {{ model }}
    WHERE pathogen_detail != 'NO_DETAIL'
    EXCEPT
    SELECT "name" AS pathogen_name, "detail" as pathogen_detail
    FROM {{ ref('pathogen_detail_accepted') }}
    
{% endtest %}