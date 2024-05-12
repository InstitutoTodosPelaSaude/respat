{% test accepted_uf_names(model, column_name) %}

    SELECT
        *
    FROM {{ model }}
    WHERE
    {{column_name}} not in (
        'ACRE','ALAGOAS','AMAPA','AMAZONAS','BAHIA','CEARA','DISTRITO FEDERAL',
        'ESPIRITO SANTO','GOIAS','MARANHAO','MATO GROSSO','MATO GROSSO DO SUL',
        'MINAS GERAIS','PARA','PARAIBA','PARANA','PERNAMBUCO','PIAUI',
        'RIO DE JANEIRO','RIO GRANDE DO NORTE','RIO GRANDE DO SUL',
        'RONDONIA','RORAIMA','SANTA CATARINA','SAO PAULO','SERGIPE','TOCANTINS',
        'NOT_REPORTED' -- Não informado
    )

{% endtest %}