{{ 
    config(
        materialized='table',
        source='warehouse'
    ) 
}}

select categories_items_name as category_name, count(*) as occurence_by_category
from {{ source('spotify_sg', 'browse_category_flatten')}}
group by categories_items_name