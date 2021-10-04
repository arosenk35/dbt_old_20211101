select 
    drug_id 
from {{ ref('stock_drug') }}