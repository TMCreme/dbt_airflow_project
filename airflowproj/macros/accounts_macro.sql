
-- CREATE FUNCTION fn_nullify_if_empty(in_string VARCHAR(255))
--   RETURNS VARCHAR(255)
--   BEGIN
--     IF in_string = ''
--       THEN RETURN NULL;
--       ELSE RETURN in_string;
--     END IF;
--   END $$
-- case when p.parent_id is null then false else true end as has_parent
-- to_char(dateCreated::timestamp at time zone 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')

{% macro convert_timezone(column, source_tz) -%}
   case
        when {{ column }} like 'NaN' 
        then replace({{column}}, {{ current_timestamp() }})
    else 
        cast({{ column }} AS {{ type_timestamp() }})
    
{%- endmacro -%}

-- {% macro string_to_datetime(dateCreated) %}
--     CASE
--     WHEN dateCreated <> 'NaN'
--       THEN NULL
--       ELSE dateCreated
-- {% endmacro %}
