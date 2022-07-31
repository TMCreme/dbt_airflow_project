with product_detail as (
    SELECT 
        try_cast(date as DATETIME) as crawldate,
        category,
        name,
        try_cast(price as FLOAT) as price,
        try_cast(percentage as FLOAT)  as discount,
        currency
    FROM
        {{ source("allproducts", "jumia_products") }}
)

SELECT * 
FROM product_detail

