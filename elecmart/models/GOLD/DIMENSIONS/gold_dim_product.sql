select product_id, product_name, dp.brand_id, db.brand_name as brand_name,
dp.category_id, dc.category_name as category_name,
dp.subcategory_id, dsc.subcategory_name as subcategory_name,
dp.unit_cost, dp.unit_price, dp.warranty_years
 from {{ ref('silver_dim_product') }} dp
 inner join {{ref('silver_dim_brand')}} db
    on dp.brand_id = db.brand_id
    inner join {{ref('silver_dim_category')}} dc
    on dp.category_id = dc.category_id
    inner join {{ref('silver_dim_subcategory')}} dsc
    on dp.subcategory_id = dsc.subcategory_id
