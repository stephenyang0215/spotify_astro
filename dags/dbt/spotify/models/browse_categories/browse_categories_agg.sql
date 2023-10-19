{{ 
    config(
        materialized='view'
    ) 
}}

select categories_items_name as category_name, count(*) as occurence_by_category
from {{ source('spotify', 'browse_categories_flatten')}}
group by categories_items_name