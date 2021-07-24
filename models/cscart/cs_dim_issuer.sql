{{
  config({
    "materialized": "view"
  })
}}
SELECT  
      company,  
      email
      firstname,
      lastname,
      user_name as issuer_name,
      user_id as issuer_id
FROM {{ ref('cs_dim_user') }} u
