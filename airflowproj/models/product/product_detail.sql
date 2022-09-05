with product_detail as (
    SELECT 
        date,
        category,
        name,
        CAST(price AS double precision) as price,
        CAST(percent AS double precision)  as discount,
        currency
    FROM
        {{ source("allproducts", "jumia_products") }}
    WHERE category!='category'
)

SELECT * 
FROM product_detail

