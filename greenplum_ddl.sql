-- =====================================================================
-- Data Vault 2.0 DDL для Greenplum
-- Проект: Хранилище данных для Sample Superstore
-- Источник данных: SampleSuperstore.csv
-- =====================================================================

-- Создание схемы для Data Vault

CREATE SCHEMA IF NOT EXISTS dv;

-- =====================================================================
-- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ДЛЯ РАБОТЫ С DATA VAULT
-- =====================================================================

-- Функция для генерации MD5 хеша (для использования в ETL процессах)
-- Использует нормализацию и обработку NULL для детерминированности
CREATE OR REPLACE FUNCTION dv.generate_hash_key(p_business_key TEXT)
RETURNS CHAR(32) AS $$
BEGIN
    -- Нормализация: COALESCE для обработки NULL, BTRIM для удаления пробелов, UPPER для регистронезависимости
    RETURN MD5(UPPER(BTRIM(COALESCE(p_business_key, '<NULL>'))));
END;
$$ LANGUAGE plpgsql IMMUTABLE;

COMMENT ON FUNCTION dv.generate_hash_key(p_business_key TEXT) IS 'Генерация MD5 хеша для бизнес-ключа с нормализацией и обработкой NULL';

-- Функция для генерации композитного хеш-ключа (для Link таблиц)
-- Улучшенная версия с защитой от коллизий
CREATE OR REPLACE FUNCTION dv.generate_composite_hash_key(p_keys TEXT[])
RETURNS CHAR(32) AS $$
DECLARE
    v_normalized TEXT[];
    v_concat TEXT;
    i INT;
BEGIN
    -- Нормализация каждого элемента массива
    FOR i IN 1..array_length(p_keys, 1) LOOP
        v_normalized[i] := UPPER(BTRIM(COALESCE(p_keys[i], '<NULL>')));
    END LOOP;
    
    -- Конкатенация с явным разделителем, который не встречается в данных
    v_concat := array_to_string(v_normalized, '^^');
    
    RETURN MD5(v_concat);
END;
$$ LANGUAGE plpgsql IMMUTABLE;

COMMENT ON FUNCTION dv.generate_composite_hash_key(p_keys text[]) IS 'Генерация MD5 хеша для композитного ключа с нормализацией и защитой от коллизий';

-- =====================================================================
-- HUB TABLES (Хабы)
-- =====================================================================

-- Хаб клиентов
CREATE TABLE dv.hub_customer (
    hub_customer_id CHAR(32) NOT NULL,      -- MD5 hash от бизнес-ключа
    customer_id VARCHAR(250) NOT NULL,        -- Бизнес-ключ из SampleSuperstore
    load_date TIMESTAMP NOT NULL,            -- Дата загрузки
    record_source VARCHAR(50) NOT NULL,      -- Источник записи
    CONSTRAINT pk_hub_customer PRIMARY KEY (hub_customer_id)
)
DISTRIBUTED BY (hub_customer_id);

ALTER TABLE dv.hub_customer
    ALTER COLUMN customer_id TYPE VARCHAR(255);




-- Индекс для ускорения поиска по бизнес-ключу
CREATE UNIQUE INDEX idx_hub_customer_bk ON dv.hub_customer(customer_id, hub_customer_id);

COMMENT ON TABLE dv.hub_customer  IS 'Хаб клиентов - содержит уникальные бизнес-ключи клиентов из Sample Superstore';

-- Хаб товаров
CREATE TABLE dv.hub_product (
    hub_product_id CHAR(32) NOT NULL,        -- MD5 hash от бизнес-ключа
    product_id VARCHAR(100) NOT NULL,        -- Бизнес-ключ из SampleSuperstore
    load_date TIMESTAMP NOT NULL,            -- Дата загрузки
    record_source VARCHAR(50) NOT NULL,      -- Источник записи
    CONSTRAINT pk_hub_product PRIMARY KEY (hub_product_id)
)
DISTRIBUTED BY (hub_product_id);

-- Индекс для ускорения поиска по бизнес-ключу
CREATE UNIQUE INDEX idx_hub_product_bk ON dv.hub_product(product_id, hub_product_id);

COMMENT ON TABLE dv.hub_product IS 'Хаб товаров - содержит уникальные бизнес-ключи товаров из Sample Superstore';

-- Хаб заказов
CREATE TABLE dv.hub_order (
    hub_order_id CHAR(32) NOT NULL,          -- MD5 hash от бизнес-ключа
    order_id VARCHAR(50) NOT NULL,           -- Бизнес-ключ из SampleSuperstore
    load_date TIMESTAMP NOT NULL,            -- Дата загрузки
    record_source VARCHAR(50) NOT NULL,      -- Источник записи
    CONSTRAINT pk_hub_order PRIMARY KEY (hub_order_id)
)
DISTRIBUTED BY (hub_order_id);

-- Индекс для ускорения поиска по бизнес-ключу
CREATE UNIQUE INDEX idx_hub_order_bk ON dv.hub_order(order_id, hub_order_id);

COMMENT ON TABLE dv.hub_order IS 'Хаб заказов - содержит уникальные бизнес-ключи заказов из Sample Superstore';

-- =====================================================================
-- LINK TABLES (Связи)
-- =====================================================================

-- Связь заказа с клиентом
CREATE TABLE dv.link_order_customer (
    link_order_customer_id CHAR(32) NOT NULL,  -- MD5 hash от композитного ключа
    hub_customer_id CHAR(32) NOT NULL,         -- FK к hub_customer
    hub_order_id CHAR(32) NOT NULL,            -- FK к hub_order
    load_date TIMESTAMP NOT NULL,              -- Дата загрузки
    record_source VARCHAR(50) NOT NULL,        -- Источник записи
    CONSTRAINT pk_link_order_customer PRIMARY KEY (link_order_customer_id),
    CONSTRAINT fk_link_order_customer_customer FOREIGN KEY (hub_customer_id) 
        REFERENCES dv.hub_customer(hub_customer_id),
    CONSTRAINT fk_link_order_customer_order FOREIGN KEY (hub_order_id) 
        REFERENCES dv.hub_order(hub_order_id)
)
DISTRIBUTED BY (link_order_customer_id);

-- Индексы для оптимизации запросов
CREATE INDEX idx_link_order_customer_cust ON dv.link_order_customer(hub_customer_id);
CREATE INDEX idx_link_order_customer_ord ON dv.link_order_customer(hub_order_id);
-- Уникальный индекс для предотвращения дубликатов связей заказ-клиент
CREATE UNIQUE INDEX idx_link_order_customer_unique ON dv.link_order_customer(hub_order_id, hub_customer_id, link_order_customer_id);

COMMENT ON TABLE dv.link_order_customer IS 'Связь между заказами и клиентами';

-- Связь заказа с товаром (позиции заказа)
CREATE TABLE dv.link_order_product (
    link_order_product_id CHAR(32) NOT NULL,   -- MD5 hash от композитного ключа
    hub_order_id CHAR(32) NOT NULL,            -- FK к hub_order
    hub_product_id CHAR(32) NOT NULL,          -- FK к hub_product
    row_id BIGINT NOT NULL,                    -- Row ID из SampleSuperstore (уникальный идентификатор строки)
    load_date TIMESTAMP NOT NULL,              -- Дата загрузки
    record_source VARCHAR(50) NOT NULL,        -- Источник записи
    CONSTRAINT pk_link_order_product PRIMARY KEY (link_order_product_id),
    CONSTRAINT fk_link_order_product_order FOREIGN KEY (hub_order_id) 
        REFERENCES dv.hub_order(hub_order_id),
    CONSTRAINT fk_link_order_product_product FOREIGN KEY (hub_product_id) 
        REFERENCES dv.hub_product(hub_product_id)
)
DISTRIBUTED BY (link_order_product_id);

-- Индексы для оптимизации запросов
CREATE INDEX idx_link_order_product_ord ON dv.link_order_product(hub_order_id);
CREATE INDEX idx_link_order_product_prod ON dv.link_order_product(hub_product_id);
CREATE UNIQUE INDEX idx_link_order_product_bk ON dv.link_order_product(row_id, link_order_product_id);

COMMENT ON TABLE dv.link_order_product IS 'Связь между заказами и товарами (позиции заказа)';

-- =====================================================================
-- SATELLITE TABLES (Сателлиты)
-- =====================================================================

-- Сателлит основных атрибутов клиента
CREATE TABLE dv.sat_customer (
    hub_customer_id CHAR(32) NOT NULL,         -- FK к hub_customer
    load_date TIMESTAMP NOT NULL,              -- Дата загрузки (часть PK)
    load_end_date TIMESTAMP,                   -- Дата окончания действия записи
    hash_diff CHAR(32) NOT NULL,               -- Hash для обнаружения изменений
    customer_name VARCHAR(255),                -- Имя клиента из SampleSuperstore
    segment VARCHAR(50),                       -- Сегмент клиента (Consumer, Corporate, Home Office)
    record_source VARCHAR(50) NOT NULL,        -- Источник записи
    CONSTRAINT pk_sat_customer PRIMARY KEY (hub_customer_id, load_date),
    CONSTRAINT fk_sat_customer_hub FOREIGN KEY (hub_customer_id) 
        REFERENCES dv.hub_customer(hub_customer_id)
)
DISTRIBUTED BY (hub_customer_id);

-- Индекс для поиска актуальной записи
CREATE INDEX idx_sat_customer_current ON dv.sat_customer(hub_customer_id, load_date) 
    WHERE load_end_date IS NULL;

COMMENT ON TABLE dv.sat_customer IS 'Сателлит основных атрибутов клиента с историей изменений';

-- Сателлит географических атрибутов клиента (разделен по частоте изменений)
CREATE TABLE dv.sat_customer_location (
    hub_customer_id CHAR(32) NOT NULL,         -- FK к hub_customer
    load_date TIMESTAMP NOT NULL,              -- Дата загрузки (часть PK)
    load_end_date TIMESTAMP,                   -- Дата окончания действия записи
    hash_diff CHAR(32) NOT NULL,               -- Hash для обнаружения изменений
    country VARCHAR(100),                      -- Страна
    region VARCHAR(50),                        -- Регион (West, East, Central, South)
    state VARCHAR(50),                         -- Штат
    city VARCHAR(100),                         -- Город
    postal_code VARCHAR(20),                   -- Почтовый индекс
    record_source VARCHAR(50) NOT NULL,        -- Источник записи
    CONSTRAINT pk_sat_customer_location PRIMARY KEY (hub_customer_id, load_date),
    CONSTRAINT fk_sat_customer_location_hub FOREIGN KEY (hub_customer_id) 
        REFERENCES dv.hub_customer(hub_customer_id)
)
DISTRIBUTED BY (hub_customer_id);

-- Индекс для поиска актуальной записи
CREATE INDEX idx_sat_customer_location_current ON dv.sat_customer_location(hub_customer_id, load_date) 
    WHERE load_end_date IS NULL;

COMMENT ON TABLE dv.sat_customer_location IS 'Сателлит географических атрибутов клиента с историей изменений';

-- Сателлит атрибутов товара
CREATE TABLE dv.sat_product (
    hub_product_id CHAR(32) NOT NULL,          -- FK к hub_product
    load_date TIMESTAMP NOT NULL,              -- Дата загрузки (часть PK)
    load_end_date TIMESTAMP,                   -- Дата окончания действия записи
    hash_diff CHAR(32) NOT NULL,               -- Hash для обнаружения изменений
    product_name VARCHAR(500),                 -- Название товара из SampleSuperstore
    category VARCHAR(100),                     -- Категория (Furniture, Office Supplies, Technology)
    sub_category VARCHAR(100),                 -- Подкатегория товара
    record_source VARCHAR(50) NOT NULL,        -- Источник записи
    CONSTRAINT pk_sat_product PRIMARY KEY (hub_product_id, load_date),
    CONSTRAINT fk_sat_product_hub FOREIGN KEY (hub_product_id) 
        REFERENCES dv.hub_product(hub_product_id)
)
DISTRIBUTED BY (hub_product_id);

-- Индекс для поиска актуальной записи
CREATE INDEX idx_sat_product_current ON dv.sat_product(hub_product_id, load_date) 
    WHERE load_end_date IS NULL;

COMMENT ON TABLE dv.sat_product IS 'Сателлит атрибутов товара с историей изменений';

-- Сателлит атрибутов заказа
CREATE TABLE dv.sat_order (
    hub_order_id CHAR(32) NOT NULL,            -- FK к hub_order
    load_date TIMESTAMP NOT NULL,              -- Дата загрузки (часть PK)
    load_end_date TIMESTAMP,                   -- Дата окончания действия записи
    hash_diff CHAR(32) NOT NULL,               -- Hash для обнаружения изменений
    ship_mode VARCHAR(50),                     -- Способ доставки (Standard Class, Second Class, First Class, Same Day)
    record_source VARCHAR(50) NOT NULL,        -- Источник записи
    CONSTRAINT pk_sat_order PRIMARY KEY (hub_order_id, load_date),
    CONSTRAINT fk_sat_order_hub FOREIGN KEY (hub_order_id) 
        REFERENCES dv.hub_order(hub_order_id)
)
DISTRIBUTED BY (hub_order_id);

-- Индекс для поиска актуальной записи
CREATE INDEX idx_sat_order_current ON dv.sat_order(hub_order_id, load_date) 
    WHERE load_end_date IS NULL;


COMMENT ON TABLE dv.sat_order IS 'Сателлит атрибутов заказа с историей изменений';

-- Сателлит атрибутов позиции заказа (транзакционные данные)
CREATE TABLE dv.sat_order_product (
    link_order_product_id CHAR(32) NOT NULL,   -- FK к link_order_product
    load_date TIMESTAMP NOT NULL,              -- Дата загрузки (часть PK)
    load_end_date TIMESTAMP,                   -- Дата окончания действия записи
    hash_diff CHAR(32) NOT NULL,               -- Hash для обнаружения изменений
    sales NUMERIC(15,2),                       -- Сумма продажи из SampleSuperstore
    quantity INT,                              -- Количество
    discount NUMERIC(5,4),                     -- Скидка (процент, 0-1)
    profit NUMERIC(15,2),                      -- Прибыль
    record_source VARCHAR(50) NOT NULL,        -- Источник записи
    CONSTRAINT pk_sat_order_product PRIMARY KEY (link_order_product_id, load_date),
    CONSTRAINT fk_sat_order_product_link FOREIGN KEY (link_order_product_id) 
        REFERENCES dv.link_order_product(link_order_product_id)
)
DISTRIBUTED BY (link_order_product_id);

-- Индекс для поиска актуальной записи
CREATE INDEX idx_sat_order_product_current ON dv.sat_order_product(link_order_product_id, load_date) 
    WHERE load_end_date IS NULL;

COMMENT ON TABLE dv.sat_order_product IS 'Сателлит транзакционных атрибутов позиций заказа с историей изменений';

-- =====================================================================
-- ПРИМЕРЫ ПРЕДСТАВЛЕНИЙ ДЛЯ АНАЛИТИЧЕСКОГО СЛОЯ (BUSINESS VAULT)
-- =====================================================================

-- Представление актуальных данных о клиентах
CREATE OR REPLACE VIEW dv.v_current_customers AS
SELECT 
    h.hub_customer_id,
    h.customer_id,
    s.customer_name,
    s.segment,
    l.country,
    l.region,
    l.state,
    l.city,
    l.postal_code,
    h.load_date as hub_load_date,
    s.load_date as sat_load_date
FROM dv.hub_customer h
INNER JOIN dv.sat_customer s ON h.hub_customer_id = s.hub_customer_id AND s.load_end_date IS NULL
LEFT JOIN dv.sat_customer_location l ON h.hub_customer_id = l.hub_customer_id AND l.load_end_date IS NULL;

COMMENT ON VIEW dv.v_current_customers IS 'Актуальное состояние атрибутов клиентов из Sample Superstore';

-- Представление актуальных данных о товарах
CREATE OR REPLACE VIEW dv.v_current_products AS
SELECT 
    h.hub_product_id,
    h.product_id,
    s.product_name,
    s.category,
    s.sub_category,
    h.load_date as hub_load_date,
    s.load_date as sat_load_date
FROM dv.hub_product h
INNER JOIN dv.sat_product s ON h.hub_product_id = s.hub_product_id
WHERE s.load_end_date IS NULL;

COMMENT ON VIEW dv.v_current_products IS 'Актуальное состояние атрибутов товаров из Sample Superstore';

-- Представление полной информации о заказах
CREATE OR REPLACE VIEW dv.v_current_orders AS
SELECT 
    ho.hub_order_id,
    ho.order_id,
    so.ship_mode,
    hc.customer_id,
    sc.customer_name,
    sc.segment,
    ho.load_date as hub_load_date,
    so.load_date as sat_load_date
FROM dv.hub_order ho
INNER JOIN dv.sat_order so ON ho.hub_order_id = so.hub_order_id
INNER JOIN dv.link_order_customer loc ON ho.hub_order_id = loc.hub_order_id
INNER JOIN dv.hub_customer hc ON loc.hub_customer_id = hc.hub_customer_id
INNER JOIN dv.sat_customer sc ON hc.hub_customer_id = sc.hub_customer_id
WHERE so.load_end_date IS NULL 
  AND sc.load_end_date IS NULL;

COMMENT ON VIEW dv.v_current_orders IS 'Актуальное состояние заказов с информацией о клиентах из Sample Superstore';

-- Представление детализации заказов (без ORDER BY в определении)
CREATE OR REPLACE VIEW dv.v_order_details AS
SELECT 
    ho.order_id,
    so.ship_mode,
    hp.product_id,
    sp.product_name,
    sp.category,
    sp.sub_category,
    sop.quantity,
    sop.sales,
    sop.discount,
    sop.profit,
    lop.row_id
FROM dv.hub_order ho
INNER JOIN dv.sat_order so ON ho.hub_order_id = so.hub_order_id
INNER JOIN dv.link_order_product lop ON ho.hub_order_id = lop.hub_order_id
INNER JOIN dv.hub_product hp ON lop.hub_product_id = hp.hub_product_id
INNER JOIN dv.sat_product sp ON hp.hub_product_id = sp.hub_product_id
INNER JOIN dv.sat_order_product sop ON lop.link_order_product_id = sop.link_order_product_id
WHERE so.load_end_date IS NULL 
  AND sp.load_end_date IS NULL 
  AND sop.load_end_date IS NULL;

COMMENT ON VIEW dv.v_order_details IS 'Детализация заказов с информацией о товарах из Sample Superstore';

COMMENT ON SCHEMA dv IS 'Data Vault 2.0 schema для хранилища данных Sample Superstore';
