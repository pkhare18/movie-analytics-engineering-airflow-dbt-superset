{{ config(materialized='table') }}
 
select distinct
    release_date,
    extract(year from release_date) as year,
    extract(month from release_date) as month,
    extract(day from release_date) as day
from {{ ref('movies_clean') }}
where release_date is not null