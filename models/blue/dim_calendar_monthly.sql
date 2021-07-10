 select generate_series('2019-01-01'::date, 
         now()::date
        ,('1 month')::interval)::date as date