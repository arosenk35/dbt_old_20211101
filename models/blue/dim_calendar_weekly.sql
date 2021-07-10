 select generate_series('2019-12-29'::date, 
         now()::date
        ,('7 day')::interval)::date as date