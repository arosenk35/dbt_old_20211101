   {{
  config({
    "materialized": "table",
    "post-hook": [
    after_commit("create index  index_{{this.name}}_on_p_id on {{this.schema}}.{{this.name}} (practice_id)"),
    after_commit("create index  index_{{this.name}}_on_d_id on {{this.schema}}.{{this.name}} (doctor_id)")
            ]
  })
  }}
with
    clinic as( 
    SELECT 
    case 
    when  address ilike '%vet%'
    or address ilike '%hosp%' 
    or address ilike '%practice%' 
    or address ilike '%clinic%' 
    or address ilike '%animal%'
    or address ilike '%center%' 
    or address ilike '%banfield%' 
    or address ilike '%service%' 
    or address ilike '%corpor%' 
    or address ilike '%pet%' 
    or address ilike '%spca%' 
    or address ilike '%health%' 
    or address ilike '%dds%' 
    or address ilike '%shelter%' 
    or address ilike '%dds%' 
    or address ilike '%medic%' 
    or address ilike '%emergen%' 
    or address ilike '%care%' 
    or address ilike '%surgery%' 
    or address ilike '%spca%' 
    or address ilike '%society%' 
    then      address
    when 		   
    note ilike '%vet%'
    or note ilike '%hosp%' 
    or note ilike '%practice%' 
    or note ilike '%clinic%' 
    or note ilike '%animal%'
    or note ilike '%center%' 
    or note ilike '%corpor%' 
    or note ilike '%pet%' 
    or note ilike '%service%' 
    or note ilike '%spca%' 	  
    or note ilike '%health%' 
    or note ilike '%dds%' 
    or note ilike '%banfield%' 
    or note ilike '%shelter%' 
    or note ilike '%dds%' 
    or note ilike '%medic%' 
    or note ilike '%emergen%' 
    or note ilike '%care%' 
    or note ilike '%surgery%' 
    or note ilike '%spca%' 
    or note ilike '%society%' 
    then note
    when 
    address2 ilike '%vet%'
    or address2 ilike '%hosp%' 
    or address2 ilike '%practice%' 
    or address2 ilike '%pet%' 
    or address2 ilike '%clinic%' 
    or address2 ilike '%anima%'
    or address2 ilike '%center%' 
    or address2 ilike '%corpor%' 
    or address2 ilike '%banfield%' 
    or address2 ilike '%health%' 
    or address2 ilike '%service%' 
    or address2 ilike '%dds%' 
    or address2 ilike '%shelter%' 
    or address2 ilike '%dds%' 
    or address2 ilike '%medic%' 
    or address2 ilike '%emergen%' 
    or address2 ilike '%care%' 
    or address2 ilike '%surgery%' 
    or address2 ilike '%spca%' 
    or address2 ilike '%society%' 
    then address2
    when nullif(address,'') is null then
    note
    else
    address
    end as practice,
    nullif(address,'')                  as address,
    nullif(address2,'')                  as address2,
    note,
    z.zipid::text                       as zip,
    phone11||phone12||phone13           as phone1,
    coalesce(z.country,'USA')           as country,
    coalesce(z.state,'CA')              as state,
    z.city,
    dm.created_date,
    active,
    dm.srno as doctor_id,
    prm.practice_id as practice_id_map

    FROM ips.doctor_master dm
    left join {{ ref('practice_map') }} prm on dm.srno = prm.doctor_id
    left join ips.zip_master z on dm.zip = z.srno and dm.zip is not null
    where  exists (select 'x' from ips.prescription p where dm.srno=p.doctor_id and p.office_id=2)
    ), 

      vet as(
        SELECT   regexp_replace(replace(replace(replace(replace(replace(lower(practice),'street','st'),'drive','dr'),'avenue','ave'),'road','rd'),'floor','fl'),'\#|\''|\`| |\,|\&|\.|-|','','g')
      as fuzzy_key,   

          btrim(practice)  as practice,
          address,
        address2,
        note,
          zip,
          phone1,
          country,
          state,
          city,
          created_date,
          active,
        doctor_id,	    practice_id_map

      FROM clinic
      ),
      
      fuzzy as (select 
            first_value(doctor_id) OVER (PARTITION BY state,city,zip,phone1 order by 
                    case when active='Y' then 1 else 99 end,   
            case when practice_id_map is not null then 1 else 99 end,
                    case when btrim(practice) is null then 99
            when practice ilike '%no%longer%' then 77
                    when  practice ilike '%vet%' then 2
                    when  practice  ilike '%hosp%' then 1 
                    when  practice  ilike '%clinic%' then 3
                    when  practice  ilike '%animal%' then 4
                    when  practice  ilike '%center%' then 5
                    when  practice  ilike '%corpor%' then 6
                    when  practice  ilike '%pets%' then 7
            when  practice  ilike '%spca%' then 8
            when  practice ilike '%society%' then 8
                    else 88
                    end,             
                    created_date asc               
                    ) first_doctor_id,	
                    doctor_id,                

                    first_value(practice) OVER (PARTITION BY state,city,zip,phone1 order by 
                    case when active='Y' then 1 else 99 end,
                    case 
            when btrim(practice) is null then 99
            when practice ilike '%no%longer%' then 77
              when practice ilike '%no%longer%' then 77
                    when practice ilike '%vet%' then 2
                    when practice ilike '%hosp%' then 1 
            when practice ilike '%practice%' then 3 
                    when practice ilike '%clinic%' then 3
                    when practice ilike '%animal%' then 4
                    when practice ilike '%center%' then 5
                    when practice ilike '%corpor%' then 6
                    when practice ilike '%pets%' then 7
                    else 88
                    end asc,											
                    created_date asc
                    ) first_practice,
          fuzzy_key,practice,	    practice_id_map
    
      FROM vet
      order by fuzzy_key
      )

      select     
          coalesce(first_doctor_id) as practice_id,
          doctor_id,
          'map' as type
        
        from fuzzy 
        where practice_id_map is null
      and nullif(practice,'') is not null

  union all

  (select   
      m.practice_id,	
      m.doctor_id,
      'csv' as type
  FROM {{ ref('practice_map') }}  m
  )