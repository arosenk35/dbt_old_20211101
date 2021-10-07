{% macro email_cleaned(field) -%}
case 
              when {{field}} ilike '%ggvcp%' then null
              when {{field}} ilike '%ggcvp%' then null
              when {{field}} not like '%@%'  then null 
              else btrim(lower({{field}}))       
end  
{%- endmacro %}
