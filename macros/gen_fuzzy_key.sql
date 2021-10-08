{% macro gen_fuzzy_key(field) -%}
  lower(regexp_replace({{field}},'|\/|\?|\@|\#|\"|\''|\$|\;|\`| |\,|\&|\.|-|','','g'))
{%- endmacro %}
