{% macro get_end_date(default='month_end') %}
    {%- set v = var('end_date', none) -%}

    {%- if v is not none and (v | trim) != '' -%}
        cast('{{ v }}' as {{ dbt.type_date() }})

    {%- else -%}
        {%- if default == 'today' -%}
            cast({{ dbt.current_timestamp() }} as {{ dbt.type_date() }})
            
        {%- else -%}
            cast( last_day(cast({{ dbt.current_timestamp() }} as {{ dbt.type_date() }})) as {{ dbt.type_date() }} )
        {%- endif -%}

    {%- endif -%}
{% endmacro %}