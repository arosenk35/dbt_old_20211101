{{
  config({
    "materialized": "table",
    "post-hook": [
      	after_commit("create index  index_{{this.name}}_on_k1 on {{this.schema}}.{{this.name}} (key1)"),
        after_commit("create index  index_{{this.name}}_on_k2 on {{this.schema}}.{{this.name}} (key2)")]
  })
}}

select
    distinct on (product_id)
    
    nullif(regexp_replace(lower(cs.product),'micro|\,|\-|\(|\)|\/| |otic|\%|s$|oral|','','g'),'') as key1,
    nullif(regexp_replace(lower(cs.product),'\(.*\)$|micro|\,|\-|\(|\)|\/| |otic|\%|s$|oral|','','g'),'') as key2,
    cs.product ,
    cs.product_id ,
    cs.price,
	cs.product_code,
    cs.status as status,
    case    when cs.status='A' then 'Active'
            when cs.status='D' then 'Disabled'
            when cs.status='H' then 'Hidden'
    end as status_name,
    TIMESTAMP 'epoch' + updated_timestamp::numeric * INTERVAL '1 second'    as updated_date,
	TIMESTAMP 'epoch' + timestamp::numeric * INTERVAL '1 second'            as created_date,
    main_pair__detailed__http_image_path                                    as image_path,	
    case 
        when 
            product ilike 'AMLODIPINE%SUSPENSION%' or
            product ilike 'ARANESP%DARBEPOETIN%INJECTION%VIAL%' or
            product ilike 'POTASSIUM%SUSPENSION%' or
            product ilike 'OMEPRAZOLE%SUSPENSION%' or
            product ilike 'TERBUTALINE%SUSPENSION%' or
            product ilike 'CHLORAMBUCIL%' or
            product ilike 'SAME/MILK%THISTLE%SUSPENSION%' or
            product ilike 'AZITHROMYCIN%SUSPENSION%' or
            product ilike 'MARBOFLOXACIN%SUSPENSION%'
        then true
        else false
    end as cold_shipping
from cscart.products cs