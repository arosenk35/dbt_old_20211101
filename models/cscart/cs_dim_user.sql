{{
  config({
    "materialized": "table",
    "post-hook": [
      	after_commit("create index  index_{{this.name}}_on_d_id on {{this.schema}}.{{this.name}} (user_id)")]
  })
}}
SELECT  
      initcap(u.company)      as company,  
      lower(u.email)          as email, 
      initcap(u.firstname)    as firstname,
      u.is_root, 
      initcap(u.lastname)     as lastname,
      btrim(initcap(coalesce(firstname,''))||' '|| initcap(nullif(lastname,''))) as user_name,
      u.status, 
         TIMESTAMP 'epoch' + timestamp::numeric * INTERVAL '1 second' as created_date,
      u.user_id,
      case 
            when u.email ilike '%ggvcp%'
            then false
            when u.email not like '%@%'
            then false
            else true
      end as valid_email
FROM cscart.users u