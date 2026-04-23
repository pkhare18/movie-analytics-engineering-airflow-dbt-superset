{{ config(materialized='table') }}
 
select
    movie_id,
    title,
    original_language,
    release_date
from {{ ref('movies_clean') }}