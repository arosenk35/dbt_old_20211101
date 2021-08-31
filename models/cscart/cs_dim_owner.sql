{{
  config({
    "materialized": "table",
    "post-hook": [
      	after_commit("create index  index_{{this.name}}_on_acct_id on {{this.schema}}.{{this.name}} (account_id)"),
		after_commit("create index  index_{{this.name}}_on_email on {{this.schema}}.{{this.name}} (email)")]
  })
}}

SELECT  distinct on(cs.account_id)
*	
FROM {{ ref('cs_der_owner_match') }}  cs

order by 
		cs.account_id,
		rank asc
