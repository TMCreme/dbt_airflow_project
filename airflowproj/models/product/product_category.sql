with product_category as (
    select 
        DISTINCT category 
    FROM 
        {{ source("allproducts", "jumia_products") }}
   ORDER BY category
)

select * 
from product_category

