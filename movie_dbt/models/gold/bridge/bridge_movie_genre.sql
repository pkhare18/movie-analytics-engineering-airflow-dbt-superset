{{ config(materialized='table') }}
 
select
    movie_id,
    unnest(
        string_to_array(
            replace(replace(genre_ids, '[', ''), ']', ''),
            ','
        )::int[]
    ) as genre_id
from {{ ref('movies_clean') }}