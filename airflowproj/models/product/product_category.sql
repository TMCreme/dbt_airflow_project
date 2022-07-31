with product_category as (
    select 
        category 
    FROM 
        {{ source("allproducts", "jumia_products") }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY category ORDER BY category) = 1
)

select * 
from product_category