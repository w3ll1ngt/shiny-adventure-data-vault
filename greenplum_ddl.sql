-- =====================================================================
-- Data Vault 2.0 DDL для Greenplum
-- Проект: Хранилище данных для системы электронной коммерции
-- =====================================================================

-- Создание схемы для Data Vault
CREATE SCHEMA IF NOT EXISTS dv;

-- =====================================================================
-- HUB TABLES (Хабы)
-- =====================================================================

-- Хаб клиентов
CREATE TABLE dv.hub_customer (
    hub_customer_id CHAR(32) NOT NULL,      -- MD5 hash от бизнес-ключа
    customer_id VARCHAR(50) NOT NULL,        -- Бизнес-ключ
    load_date TIMESTAMP NOT NULL,            -- Дата загрузки
    record_source VARCHAR(50) NOT NULL,      -- Источник записи
    CONSTRAINT pk_hub_customer PRIMARY KEY (hub_customer_id)
)
DISTRIBUTED BY (hub_customer_id);

-- Индекс для ускорения поиска по бизнес-ключу
CREATE UNIQUE INDEX idx_hub_customer_bk ON dv.hub_customer(customer_id);

-- Хаб товаров
CREATE TABLE dv.hub_product (
    hub_product_id CHAR(32) NOT NULL,        -- MD5 hash от бизнес-ключа
    product_id VARCHAR(50) NOT NULL,         -- Бизнес-ключ
    load_date TIMESTAMP NOT NULL,            -- Дата загрузки
    record_source VARCHAR(50) NOT NULL,      -- Источник записи
    CONSTRAINT pk_hub_product PRIMARY KEY (hub_product_id)
)
DISTRIBUTED BY (hub_product_id);

-- Индекс для ускорения поиска по бизнес-ключу
CREATE UNIQUE INDEX idx_hub_product_bk ON dv.hub_product(product_id);

-- Хаб заказов
CREATE TABLE dv.hub_order (
    hub_order_id CHAR(32) NOT NULL,          -- MD5 hash от бизнес-ключа
    order_id VARCHAR(50) NOT NULL,           -- Бизнес-ключ
    load_date TIMESTAMP NOT NULL,            -- Дата загрузки
    record_source VARCHAR(50) NOT NULL,      -- Источник записи
    CONSTRAINT pk_hub_order PRIMARY KEY (hub_order_id)
)
DISTRIBUTED BY (hub_order_id);

-- Индекс для ускорения поиска по бизнес-ключу
CREATE UNIQUE INDEX idx_hub_order_bk ON dv.hub_order(order_id);

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

-- Связь заказа с товаром (позиции заказа)
CREATE TABLE dv.link_order_product (
    link_order_product_id CHAR(32) NOT NULL,   -- MD5 hash от композитного ключа
    hub_order_id CHAR(32) NOT NULL,            -- FK к hub_order
    hub_product_id CHAR(32) NOT NULL,          -- FK к hub_product
    order_item_sequence INT NOT NULL,          -- Номер позиции в заказе
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
CREATE UNIQUE INDEX idx_link_order_product_bk ON dv.link_order_product(hub_order_id, hub_product_id, order_item_sequence);

-- =====================================================================
-- SATELLITE TABLES (Сателлиты)
-- =====================================================================

-- Сателлит атрибутов клиента
CREATE TABLE dv.sat_customer (
    hub_customer_id CHAR(32) NOT NULL,         -- FK к hub_customer
    load_date TIMESTAMP NOT NULL,              -- Дата загрузки (часть PK)
    load_end_date TIMESTAMP,                   -- Дата окончания действия записи
    hash_diff CHAR(32) NOT NULL,               -- Hash для обнаружения изменений
    first_name VARCHAR(100),                   -- Имя
    last_name VARCHAR(100),                    -- Фамилия
    email VARCHAR(255),                        -- Email
    phone VARCHAR(20),                         -- Телефон
    address VARCHAR(500),                      -- Адрес
    city VARCHAR(100),                         -- Город
    country VARCHAR(100),                      -- Страна
    registration_date DATE,                    -- Дата регистрации
    record_source VARCHAR(50) NOT NULL,        -- Источник записи
    CONSTRAINT pk_sat_customer PRIMARY KEY (hub_customer_id, load_date),
    CONSTRAINT fk_sat_customer_hub FOREIGN KEY (hub_customer_id) 
        REFERENCES dv.hub_customer(hub_customer_id)
)
DISTRIBUTED BY (hub_customer_id);

-- Индекс для поиска актуальной записи
CREATE INDEX idx_sat_customer_current ON dv.sat_customer(hub_customer_id, load_date) 
    WHERE load_end_date IS NULL;

-- Сателлит атрибутов товара
CREATE TABLE dv.sat_product (
    hub_product_id CHAR(32) NOT NULL,          -- FK к hub_product
    load_date TIMESTAMP NOT NULL,              -- Дата загрузки (часть PK)
    load_end_date TIMESTAMP,                   -- Дата окончания действия записи
    hash_diff CHAR(32) NOT NULL,               -- Hash для обнаружения изменений
    product_name VARCHAR(255),                 -- Название товара
    category VARCHAR(100),                     -- Категория
    brand VARCHAR(100),                        -- Бренд
    price NUMERIC(15,2),                       -- Цена
    description TEXT,                          -- Описание
    record_source VARCHAR(50) NOT NULL,        -- Источник записи
    CONSTRAINT pk_sat_product PRIMARY KEY (hub_product_id, load_date),
    CONSTRAINT fk_sat_product_hub FOREIGN KEY (hub_product_id) 
        REFERENCES dv.hub_product(hub_product_id)
)
DISTRIBUTED BY (hub_product_id);

-- Индекс для поиска актуальной записи
CREATE INDEX idx_sat_product_current ON dv.sat_product(hub_product_id, load_date) 
    WHERE load_end_date IS NULL;

-- Сателлит атрибутов заказа
CREATE TABLE dv.sat_order (
    hub_order_id CHAR(32) NOT NULL,            -- FK к hub_order
    load_date TIMESTAMP NOT NULL,              -- Дата загрузки (часть PK)
    load_end_date TIMESTAMP,                   -- Дата окончания действия записи
    hash_diff CHAR(32) NOT NULL,               -- Hash для обнаружения изменений
    order_date TIMESTAMP,                      -- Дата заказа
    order_status VARCHAR(50),                  -- Статус заказа
    total_amount NUMERIC(15,2),                -- Общая сумма
    shipping_address VARCHAR(500),             -- Адрес доставки
    record_source VARCHAR(50) NOT NULL,        -- Источник записи
    CONSTRAINT pk_sat_order PRIMARY KEY (hub_order_id, load_date),
    CONSTRAINT fk_sat_order_hub FOREIGN KEY (hub_order_id) 
        REFERENCES dv.hub_order(hub_order_id)
)
DISTRIBUTED BY (hub_order_id);

-- Индекс для поиска актуальной записи
CREATE INDEX idx_sat_order_current ON dv.sat_order(hub_order_id, load_date) 
    WHERE load_end_date IS NULL;

-- Индекс для аналитических запросов по дате заказа
CREATE INDEX idx_sat_order_date ON dv.sat_order(order_date) 
    WHERE load_end_date IS NULL;

-- Сателлит атрибутов позиции заказа
CREATE TABLE dv.sat_order_product (
    link_order_product_id CHAR(32) NOT NULL,   -- FK к link_order_product
    load_date TIMESTAMP NOT NULL,              -- Дата загрузки (часть PK)
    load_end_date TIMESTAMP,                   -- Дата окончания действия записи
    hash_diff CHAR(32) NOT NULL,               -- Hash для обнаружения изменений
    quantity INT,                              -- Количество
    unit_price NUMERIC(15,2),                  -- Цена за единицу
    discount NUMERIC(5,2),                     -- Скидка (процент)
    line_total NUMERIC(15,2),                  -- Сумма по позиции
    record_source VARCHAR(50) NOT NULL,        -- Источник записи
    CONSTRAINT pk_sat_order_product PRIMARY KEY (link_order_product_id, load_date),
    CONSTRAINT fk_sat_order_product_link FOREIGN KEY (link_order_product_id) 
        REFERENCES dv.link_order_product(link_order_product_id)
)
DISTRIBUTED BY (link_order_product_id);

-- Индекс для поиска актуальной записи
CREATE INDEX idx_sat_order_product_current ON dv.sat_order_product(link_order_product_id, load_date) 
    WHERE load_end_date IS NULL;

-- =====================================================================
-- КОММЕНТАРИИ К ТАБЛИЦАМ
-- =====================================================================

COMMENT ON SCHEMA dv IS 'Data Vault 2.0 schema для хранилища данных';

COMMENT ON TABLE dv.hub_customer IS 'Хаб клиентов - содержит уникальные бизнес-ключи клиентов';
COMMENT ON TABLE dv.hub_product IS 'Хаб товаров - содержит уникальные бизнес-ключи товаров';
COMMENT ON TABLE dv.hub_order IS 'Хаб заказов - содержит уникальные бизнес-ключи заказов';

COMMENT ON TABLE dv.link_order_customer IS 'Связь между заказами и клиентами';
COMMENT ON TABLE dv.link_order_product IS 'Связь между заказами и товарами (позиции заказа)';

COMMENT ON TABLE dv.sat_customer IS 'Сателлит атрибутов клиента с историей изменений';
COMMENT ON TABLE dv.sat_product IS 'Сателлит атрибутов товара с историей изменений';
COMMENT ON TABLE dv.sat_order IS 'Сателлит атрибутов заказа с историей изменений';
COMMENT ON TABLE dv.sat_order_product IS 'Сателлит атрибутов позиций заказа с историей изменений';

-- =====================================================================
-- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ДЛЯ РАБОТЫ С DATA VAULT
-- =====================================================================

-- Функция для генерации MD5 хеша (для использования в ETL процессах)
CREATE OR REPLACE FUNCTION dv.generate_hash_key(p_business_key TEXT)
RETURNS CHAR(32) AS $$
BEGIN
    RETURN MD5(UPPER(TRIM(p_business_key)));
END;
$$ LANGUAGE plpgsql IMMUTABLE;

COMMENT ON FUNCTION dv.generate_hash_key IS 'Генерация MD5 хеша для бизнес-ключа';

-- Функция для генерации композитного хеш-ключа (для Link таблиц)
CREATE OR REPLACE FUNCTION dv.generate_composite_hash_key(p_keys TEXT[])
RETURNS CHAR(32) AS $$
DECLARE
    v_concat TEXT;
BEGIN
    v_concat := array_to_string(p_keys, '||');
    RETURN MD5(UPPER(TRIM(v_concat)));
END;
$$ LANGUAGE plpgsql IMMUTABLE;

COMMENT ON FUNCTION dv.generate_composite_hash_key IS 'Генерация MD5 хеша для композитного ключа';

-- =====================================================================
-- ПРИМЕРЫ ПРЕДСТАВЛЕНИЙ ДЛЯ АНАЛИТИЧЕСКОГО СЛОЯ (BUSINESS VAULT)
-- =====================================================================

-- Представление актуальных данных о клиентах
CREATE OR REPLACE VIEW dv.v_current_customers AS
SELECT 
    h.hub_customer_id,
    h.customer_id,
    s.first_name,
    s.last_name,
    s.email,
    s.phone,
    s.address,
    s.city,
    s.country,
    s.registration_date,
    h.load_date as hub_load_date,
    s.load_date as sat_load_date
FROM dv.hub_customer h
INNER JOIN dv.sat_customer s ON h.hub_customer_id = s.hub_customer_id
WHERE s.load_end_date IS NULL;

COMMENT ON VIEW dv.v_current_customers IS 'Актуальное состояние атрибутов клиентов';

-- Представление актуальных данных о товарах
CREATE OR REPLACE VIEW dv.v_current_products AS
SELECT 
    h.hub_product_id,
    h.product_id,
    s.product_name,
    s.category,
    s.brand,
    s.price,
    s.description,
    h.load_date as hub_load_date,
    s.load_date as sat_load_date
FROM dv.hub_product h
INNER JOIN dv.sat_product s ON h.hub_product_id = s.hub_product_id
WHERE s.load_end_date IS NULL;

COMMENT ON VIEW dv.v_current_products IS 'Актуальное состояние атрибутов товаров';

-- Представление полной информации о заказах
CREATE OR REPLACE VIEW dv.v_current_orders AS
SELECT 
    ho.hub_order_id,
    ho.order_id,
    so.order_date,
    so.order_status,
    so.total_amount,
    so.shipping_address,
    hc.customer_id,
    sc.first_name || ' ' || sc.last_name as customer_name,
    sc.email as customer_email,
    ho.load_date as hub_load_date,
    so.load_date as sat_load_date
FROM dv.hub_order ho
INNER JOIN dv.sat_order so ON ho.hub_order_id = so.hub_order_id
INNER JOIN dv.link_order_customer loc ON ho.hub_order_id = loc.hub_order_id
INNER JOIN dv.hub_customer hc ON loc.hub_customer_id = hc.hub_customer_id
INNER JOIN dv.sat_customer sc ON hc.hub_customer_id = sc.hub_customer_id
WHERE so.load_end_date IS NULL 
  AND sc.load_end_date IS NULL;

COMMENT ON VIEW dv.v_current_orders IS 'Актуальное состояние заказов с информацией о клиентах';

-- Представление детализации заказов
CREATE OR REPLACE VIEW dv.v_order_details AS
SELECT 
    ho.order_id,
    so.order_date,
    hp.product_id,
    sp.product_name,
    sp.category,
    sp.brand,
    sop.quantity,
    sop.unit_price,
    sop.discount,
    sop.line_total
FROM dv.hub_order ho
INNER JOIN dv.sat_order so ON ho.hub_order_id = so.hub_order_id
INNER JOIN dv.link_order_product lop ON ho.hub_order_id = lop.hub_order_id
INNER JOIN dv.hub_product hp ON lop.hub_product_id = hp.hub_product_id
INNER JOIN dv.sat_product sp ON hp.hub_product_id = sp.hub_product_id
INNER JOIN dv.sat_order_product sop ON lop.link_order_product_id = sop.link_order_product_id
WHERE so.load_end_date IS NULL 
  AND sp.load_end_date IS NULL 
  AND sop.load_end_date IS NULL
ORDER BY ho.order_id, lop.order_item_sequence;

COMMENT ON VIEW dv.v_order_details IS 'Детализация заказов с информацией о товарах';

-- =====================================================================
-- ПРИМЕРЫ INSERT STATEMENTS
-- =====================================================================

-- Пример вставки данных в хаб клиента
-- INSERT INTO dv.hub_customer (hub_customer_id, customer_id, load_date, record_source)
-- VALUES (
--     dv.generate_hash_key('CUST-001'),
--     'CUST-001',
--     CURRENT_TIMESTAMP,
--     'SOURCE_SYSTEM'
-- );

-- Пример вставки данных в сателлит клиента
-- INSERT INTO dv.sat_customer (
--     hub_customer_id, load_date, load_end_date, hash_diff,
--     first_name, last_name, email, phone, 
--     address, city, country, registration_date, record_source
-- )
-- VALUES (
--     dv.generate_hash_key('CUST-001'),
--     CURRENT_TIMESTAMP,
--     NULL,
--     MD5('John||Doe||john.doe@email.com||...'),
--     'John',
--     'Doe',
--     'john.doe@email.com',
--     '+1234567890',
--     '123 Main St',
--     'New York',
--     'USA',
--     '2024-01-15',
--     'SOURCE_SYSTEM'
-- );
