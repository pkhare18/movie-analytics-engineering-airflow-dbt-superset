{{ config(materialized='table') }}
 
select distinct
    source_type
from {{ ref('movies_clean') }}