
create table dv.sample_superstore
(
    "Ship Mode"    text,
    segment        text,
    country        text,
    city           text,
    state          text,
    "Postal Code"  integer,
    region         text,
    category       text,
    "Sub-Category" text,
    sales          text,
    quantity       text,
    discount       text,
    profit         text
)
    distributed by ("Ship Mode");

alter table dv.sample_superstore
    owner to user1;


-- imported via datagrip here

SELECT generate_hash_key(segment || '|' || country || '|' || city || '|' || state || '|' || COALESCE(("Postal Code")::text, 'NULL'))
FROM dv.sample_superstore
LIMIT 5;

-- Hub Customer
INSERT INTO dv.hub_customer (hub_customer_id, customer_id, load_date, record_source)
SELECT DISTINCT
    generate_hash_key(segment || '|' || country || '|' || city || '|' || state || '|' || COALESCE(("Postal Code")::text, 'NULL')),
    segment || '|' || country || '|' || city || '|' || state || '|' || COALESCE(("Postal Code")::text, 'NULL'),
    now(),
    'sample_superstore'
FROM dv.sample_superstore;


-- Hub Product
INSERT INTO dv.hub_product (hub_product_id, product_id, load_date, record_source)
SELECT DISTINCT
    generate_hash_key(category || '|' || "Sub-Category"),
    category || '|' || "Sub-Category",
    now(),
    'SampleSuperstore'
FROM sample_superstore;

-- Hub Order
INSERT INTO dv.hub_order (hub_order_id, order_id, load_date, record_source)
SELECT DISTINCT
    generate_hash_key("Ship Mode" || '|' || city || '|' || COALESCE("Postal Code"::text,'NULL')),
    "Ship Mode" || '|' || city || '|' || COALESCE("Postal Code"::text,'NULL'),
    now(),
    'SampleSuperstore'
FROM sample_superstore;

-- Link Order → Customer
INSERT INTO dv.link_order_customer (link_order_customer_id, hub_customer_id, hub_order_id, load_date, record_source)
SELECT DISTINCT
    generate_composite_hash_key(ARRAY[h.hub_order_id, c.hub_customer_id]),
    c.hub_customer_id,
    h.hub_order_id,
    now(),
    'SampleSuperstore'
FROM dv.hub_order h
         JOIN dv.hub_customer c ON true;

-- Link Order → Product
INSERT INTO dv.link_order_product (link_order_product_id, hub_order_id, hub_product_id, row_id, load_date, record_source)
SELECT
    generate_composite_hash_key(ARRAY[
        generate_hash_key(st."Ship Mode" || '|' || st.city || '|' || COALESCE(st."Postal Code"::text, 'NULL')),
        generate_hash_key(st.category || '|' || st."Sub-Category"),
                row_number() OVER ()::text
        ]),
    generate_hash_key(st."Ship Mode" || '|' || st.city || '|' || COALESCE(st."Postal Code"::text, 'NULL')),
    generate_hash_key(st.category || '|' || st."Sub-Category"),
    row_number() OVER (),
    now(),
    'SampleSuperstore'
FROM sample_superstore st;



-- Satellite Customer (core attributes)
INSERT INTO dv.sat_customer (hub_customer_id, load_date, load_end_date, hash_diff,
                             customer_name, segment, record_source)
SELECT DISTINCT
    h.hub_customer_id,
    now(),
    NULL::timestamp,
    generate_hash_key(segment),
    segment AS customer_name,
    segment,
    'SampleSuperstore'
FROM sample_superstore st
         JOIN dv.hub_customer h
              ON h.customer_id = segment || '|' || country || '|' || city || '|' || state || '|' || COALESCE(("Postal Code")::text,'NULL');



--satellite Customer Location

INSERT INTO dv.sat_customer_location (hub_customer_id, load_date, load_end_date, hash_diff,
                                      country, region, state, city, postal_code, record_source)
SELECT DISTINCT
    h.hub_customer_id,
    now(),
    NULL::timestamp,
    generate_hash_key(country || region || state || city || COALESCE("Postal Code"::text,'NULL')),
    country, region, state, city, "Postal Code",
    'SampleSuperstore'
FROM sample_superstore st
         JOIN dv.hub_customer h
              ON h.customer_id = segment || '|' || country || '|' || city || '|' || state || '|' || COALESCE("Postal Code"::text,'NULL');


-- satellite product
INSERT INTO dv.sat_product (hub_product_id, load_date, load_end_date, hash_diff,
                            product_name, category, sub_category, record_source)
SELECT DISTINCT
    p.hub_product_id,
    now(),
    NULL::timestamp,
    generate_hash_key(category || "Sub-Category"),
    "Sub-Category" AS product_name,
    category,
    "Sub-Category",
    'SampleSuperstore'
FROM sample_superstore st
         JOIN dv.hub_product p
              ON p.product_id = category || '|' || "Sub-Category";


-- order
INSERT INTO dv.sat_order (hub_order_id, load_date, load_end_date, hash_diff,
                        ship_mode, record_source)
SELECT DISTINCT
    h.hub_order_id,
    now(),
    NULL::timestamp,
    generate_hash_key("Ship Mode"),
    "Ship Mode",
    'SampleSuperstore'
FROM sample_superstore st
         JOIN dv.hub_order h
              ON h.order_id = "Ship Mode" || '|' || city || '|' || COALESCE("Postal Code"::text,'NULL');

-- Order Product
INSERT INTO dv.sat_order_product (link_order_product_id
     , load_date
     , load_end_date
     , hash_diff
     , sales
     , quantity
     , discount
     , profit
     , record_source)
SELECT
    generate_composite_hash_key(ARRAY[
        generate_hash_key(st."Ship Mode" || '|' || st.city || '|' || COALESCE(st."Postal Code"::text,'NULL')),
        generate_hash_key(st.category || '|' || st."Sub-Category"),
                row_number() OVER ()::text
        ]),
    now(),
    NULL,
    generate_hash_key(sales::text || quantity::text || discount::text || profit::text),
    sales::numeric, quantity::integer, discount::numeric, profit::numeric,
    'SampleSuperstore'
FROM sample_superstore st;



-- check
SELECT COUNT(*) FROM dv.hub_customer;
SELECT COUNT(*) FROM dv.hub_product;
SELECT COUNT(*) FROM dv.hub_order;
SELECT COUNT(*) FROM dv.sat_order_product;


-- readme examples
-- 
SELECT * FROM dv.v_current_customers
WHERE customer_id = 'Home Office|United States|Great Falls|Montana|59405';


-- Анализ продаж по категориям
-- 
SELECT
    category,
    sub_category,
    SUM(sales) as total_sales,
    SUM(profit) as total_profit,
    SUM(quantity) as total_quantity
FROM dv.v_order_details
GROUP BY category, sub_category
ORDER BY total_profit DESC;


-- Детализация заказа
-- 
SELECT * FROM dv.v_order_details
WHERE order_id = 'Second Class|Henderson|42420'
ORDER BY row_id;


-- История изменений товара
-- 
SELECT
    h.product_id,
    s.product_name,
    s.category,
    s.sub_category,
    s.load_date as valid_from,
    COALESCE(s.load_end_date, '9999-12-31'::timestamp) as valid_to
FROM dv.hub_product h
         JOIN dv.sat_product s ON h.hub_product_id = s.hub_product_id
WHERE h.product_id = 'FUR-BO-10001798'
ORDER BY s.load_date;