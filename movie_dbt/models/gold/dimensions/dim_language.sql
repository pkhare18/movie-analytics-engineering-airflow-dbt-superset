{{ config(materialized='table') }}
 
select distinct
    original_language as language_code
from {{ ref('movies_clean') }}