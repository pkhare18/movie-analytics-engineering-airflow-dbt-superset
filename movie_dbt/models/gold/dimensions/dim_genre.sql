{{ config(materialized='table') }}
 
with exploded as (
 
    select
        unnest(
            string_to_array(
                replace(replace(genre_ids, '[', ''), ']', ''),
                ','
            )::int[]
        ) as genre_id
 
    from {{ ref('movies_clean') }}
 
)
 
select distinct
    genre_id,
    'Genre_' || genre_id as genre_name
from exploded