{% test accepted_pathogen_names(model) %}

    SELECT
        pathogen_name, pathogen_type
    FROM {{ model }}
    EXCEPT
    SELECT "name" AS pathogen_name, "type" as pathogen_type
    FROM {{ ref('pathogens_accepted') }}
    
{% endtest %}