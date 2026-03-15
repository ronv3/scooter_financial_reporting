{% macro get_start_date() %}
    {%- set v = var('start_date', none) -%}

    {%- if v is not none and (v | trim) != '' -%}
        cast('{{ v }}' as {{ dbt.type_date() }})

    {%- else -%}
        cast( {{ dbt.date_trunc('month', dbt.current_timestamp()) }} as {{ dbt.type_date() }} )

    {%- endif -%}
{% endmacro %}