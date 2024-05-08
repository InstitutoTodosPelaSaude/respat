
{% macro 
    map_state_code_to_state_name(
        column_name,
        default_value='NULL'
    ) 
%}

{%
    set state_code_to_state_name = {
        'AC': 'ACRE',
        'AL': 'ALAGOAS',
        'AP': 'AMAPA',
        'AM': 'AMAZONAS',
        'BA': 'BAHIA',
        'CE': 'CEARA',
        'DF': 'DISTRITO FEDERAL',
        'ES': 'ESPIRITO SANTO',
        'GO': 'GOIAS',
        'MA': 'MARANHAO',
        'MT': 'MATO GROSSO',
        'MS': 'MATO GROSSO DO SUL',
        'MG': 'MINAS GERAIS',
        'PA': 'PARA',
        'PB': 'PARAIBA',
        'PR': 'PARANA',
        'PE': 'PERNAMBUCO',
        'PI': 'PIAUI',
        'RJ': 'RIO DE JANEIRO',
        'RN': 'RIO GRANDE DO NORTE',
        'RS': 'RIO GRANDE DO SUL',
        'RO': 'RONDONIA',
        'RR': 'RORAIMA',
        'SC': 'SANTA CATARINA',
        'SP': 'SAO PAULO',
        'SE': 'SERGIPE',
        'TO': 'TOCANTINS',
    }
%}

CASE
    {% for state_code, state_name in state_code_to_state_name.items() %}
    WHEN {{column_name}} = '{{ state_code }}' THEN '{{ state_name }}'
    {% endfor %}
    ELSE {{ default_value }}
END

{% endmacro %}