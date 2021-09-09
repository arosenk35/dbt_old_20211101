{{
  config({
    "materialized": "table",
    "post-hook": [
    after_commit("create index  index_{{this.name}}_on_acct_id on {{this.schema}}.{{this.name}} (account_id)"),
		after_commit("create index  index_{{this.name}}_on_email on {{this.schema}}.{{this.name}} (email)"),
    after_commit("create index  index_{{this.name}}_on_phone on {{this.schema}}.{{this.name}} using gin (array_phone)"),
		after_commit("create index  index_{{this.name}}_on_lastname on {{this.schema}}.{{this.name}}  (lastname)")]
  })
}}

SELECT  distinct on(cs.account_id)
cs.*	
FROM {{ ref('md_der_owner_match') }}  cs

order by 
		cs.account_id,
		rank asc