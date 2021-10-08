select 
organization_id,
name 
from 
{{ ref('organization_map') }}