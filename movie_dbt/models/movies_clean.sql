{{ config(materialized='table') }}
 
WITH source AS (
 
    SELECT * FROM bronze.raw_movies
 
),
 
cleaned AS (
 
    SELECT
        movie_id,
 
        -- Trim and clean title
        TRIM(title) AS title,
 
        -- Ensure positive values
        CASE
            WHEN popularity < 0 THEN NULL
            ELSE popularity
        END AS popularity,
 
        CASE
            WHEN vote_count < 0 THEN 0
            ELSE vote_count
        END AS vote_count,
 
        CASE
            WHEN vote_average < 0 THEN NULL
            WHEN vote_average > 10 THEN 10
            ELSE vote_average
        END AS vote_average,
 
        release_date,
 
        -- Standardize language
        LOWER(original_language) AS original_language
 
    FROM source
 
),
 
final AS (
 
    SELECT *
    FROM cleaned
    WHERE title IS NOT NULL
      AND movie_id IS NOT NULL
 
)
 
SELECT * FROM final