select row_to_json(j)::jsonb as "record",
current_timestamp  at time zone 'america/new_york' as changed_date
from( select email,data.owner::jsonb||json_build_object('patients',data.patients::jsonb)::jsonb as "dataFields" from
        (select  o.email,
		json_build_object(
			'ips_account_id',o.account_id,
			'phoneNumber' ,'+1'||o.phone1,
                        'owner_name',o.owner_name,
			'firstname',o.firstname,
			'lastname',o.lastname,
                        'address',o.address,
                        'zip',o.zip ,
                        'account_type',o.account_type,
                        'address2',coalesce(o.address2,''),
                        'country', o.country ,
                        'state',o.state ,
                        'city',o.city ) as owner,
                (select jsonb_agg((select x from (select
                                ips_patient_id,
                                patient_name,
                                dob,
                                sex,
                                species,
                                doctor,
				dod
                                )x))
                                FROM
                                (SELECT 
                                        p.patient_name,
                                        p.patient_id as ips_patient_id,
                                        json_build_object(
                                        'doctor_name', v.vet,
                                        'firstname', v.firstname,
                                        'lastname', v.lastname,
                                        'email',  case 
                                            when v.email ilike '%ggvcp%' then ''
                                            else  v.email 
                                            end,
                                                'address',v.address,
                                                'address2',coalesce(v.address2,''),
                                                'zip',v.zip,
                                                'phoneNumber', '+1'||v.phone1,
                                                'city',v.city,
                                                'state',v.state,
                                                'country',v.country,
                                                'ips_doctor_id',v.doctor_id,
                                                'clinic', coalesce(v.clinic,''),
                                                'sln',coalesce(sln,'')
                                        ) doctor ,  
                                        	p.dob as dob,
		                                to_char(p.dob,'MM-dd') as dobString,
                                          	coalesce(p.sex,'')    as sex,
                                          	coalesce(species,'')  as species,
						p.dod
                                FROM    analytics_blue.dim_patient p
                                        join  analytics_blue.dim_vet v on p.doctor_id=v.doctor_id
                where p.account_id=o.account_id) patient_row
                )::jsonb  AS patients
        from analytics_blue.dim_owner o
        left join analytics_blue.segment_owner so on so.account_id=o.account_id
       	where o.email is not null
	        and account_type='Patient'
		and active
        limit 1
	) data) j