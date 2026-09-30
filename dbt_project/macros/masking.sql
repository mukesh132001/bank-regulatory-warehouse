-- Reusable PII masking rules. Every mart masks the same way.

{% macro mask_account_number(column) -%}
    '******' || right({{ column }}, 4)
{%- endmacro %}

{% macro mask_name(first_name, last_name) -%}
    left({{ first_name }}, 1) || '*** ' || left({{ last_name }}, 1) || '***'
{%- endmacro %}

{% macro mask_email(column) -%}
    case when {{ column }} is null then null
         else left({{ column }}, 2) || '***@' || split_part({{ column }}, '@', 2)
    end
{%- endmacro %}
