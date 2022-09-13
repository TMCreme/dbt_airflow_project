{% macro convert_timezone(column, source_tz) -%}

        cast({{ column }} AS {{ type_timestamp() }})
{%- endmacro -%}


