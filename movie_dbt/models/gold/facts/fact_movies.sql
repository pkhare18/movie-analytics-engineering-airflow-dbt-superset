{{ config(materialized='table') }}
 
select
    movie_id,
    release_date,
    source_type,
 
    popularity,
    vote_count,
    vote_average,
 
    -- derived metrics
    (popularity * vote_average) as popularity_score,
 
    case
        when vote_average >= 8 then 'hit'
        when vote_average >= 6 then 'average'
        else 'low'
    end as movie_category,
 
    -- ranking per source
    rank() over (
        partition by source_type
        order by popularity desc
    ) as rank_in_source
 
from {{ ref('movies_clean') }}