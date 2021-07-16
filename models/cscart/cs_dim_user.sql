SELECT  
      u.company,  
      u.email, 
      u.firstname,
      u.is_root, 
      u.lastname, 
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