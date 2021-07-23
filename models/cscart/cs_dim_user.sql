SELECT  
      initcap(u.company)      as company,  
      lower(u.email)          as email, 
      initcap(u.firstname)    as firstname,
      u.is_root, 
      initcap(u.lastname)     as lastname, 
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