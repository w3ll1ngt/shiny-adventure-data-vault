# Проектирование хранилища данных методом Data Vault

## 1. Описание бизнес-контекста

Данное хранилище данных проектируется для анализа данных розничных продаж компании Superstore.

### Исходные данные

Источником данных является файл **SampleSuperstore.csv** - транзакционные данные розничных продаж со следующей структурой:

**Основные поля датасета:**
- Row ID - уникальный идентификатор строки (записи транзакции)
- Order ID - идентификатор заказа
- Order Date - дата размещения заказа
- Ship Date - дата отгрузки
- Ship Mode - способ доставки (Standard Class, Second Class, First Class, Same Day)
- Customer ID - идентификатор клиента
- Customer Name - имя клиента
- Segment - сегмент клиента (Consumer, Corporate, Home Office)
- Country - страна (United States)
- City - город
- State - штат
- Postal Code - почтовый индекс
- Region - регион (West, East, Central, South)
- Product ID - идентификатор товара
- Category - категория товара (Furniture, Office Supplies, Technology)
- Sub-Category - подкатегория товара
- Product Name - наименование товара
- Sales - сумма продажи
- Quantity - количество
- Discount - скидка (процент)
- Profit - прибыль

**Особенности датасета:**
- Каждая строка представляет одну позицию товара в заказе
- Один заказ может содержать несколько строк (разные товары)
- Данные уже денормализованы (содержат повторяющуюся информацию о клиентах, товарах и заказах)

## 2. Архитектура Data Vault

### 2.1 Hub Tables (Хабы)

Хабы содержат уникальные бизнес-ключи и метаданные загрузки:

1. **HUB_CUSTOMER** - Хаб клиентов
   - hub_customer_id (PK, hash key)
   - customer_id (бизнес-ключ)
   - load_date (дата загрузки)
   - record_source (источник записи)

2. **HUB_PRODUCT** - Хаб товаров
   - hub_product_id (PK, hash key)
   - product_id (бизнес-ключ)
   - load_date
   - record_source

3. **HUB_ORDER** - Хаб заказов
   - hub_order_id (PK, hash key)
   - order_id (бизнес-ключ)
   - load_date
   - record_source

### 2.2 Link Tables (Связи)

Связи представляют отношения между бизнес-ключами:

1. **LINK_ORDER_CUSTOMER** - Связь заказа с клиентом
   - link_order_customer_id (PK, hash key)
   - hub_customer_id (FK)
   - hub_order_id (FK)
   - load_date
   - record_source

2. **LINK_ORDER_PRODUCT** - Связь заказа с товаром (позиции заказа)
   - link_order_product_id (PK, hash key)
   - hub_order_id (FK)
   - hub_product_id (FK)
   - row_id (бизнес-ключ строки из источника)
   - load_date
   - record_source

### 2.3 Satellite Tables (Сателлиты)

Сателлиты содержат описательные атрибуты и историю изменений:

1. **SAT_CUSTOMER** - Атрибуты клиента
   - hub_customer_id (PK, FK)
   - load_date (PK)
   - load_end_date
   - hash_diff (хеш для обнаружения изменений)
   - customer_name
   - segment
   - record_source

2. **SAT_CUSTOMER_LOCATION** - Географические атрибуты клиента
   - hub_customer_id (PK, FK)
   - load_date (PK)
   - load_end_date
   - hash_diff
   - country
   - region
   - state
   - city
   - postal_code
   - record_source

3. **SAT_PRODUCT** - Атрибуты товара
   - hub_product_id (PK, FK)
   - load_date (PK)
   - load_end_date
   - hash_diff
   - product_name
   - category
   - sub_category
   - record_source

4. **SAT_ORDER** - Атрибуты заказа
   - hub_order_id (PK, FK)
   - load_date (PK)
   - load_end_date
   - hash_diff
   - order_date
   - ship_date
   - ship_mode
   - record_source

5. **SAT_ORDER_PRODUCT** - Атрибуты позиции заказа (транзакционные данные)
   - link_order_product_id (PK, FK)
   - load_date (PK)
   - load_end_date
   - hash_diff
   - sales
   - quantity
   - discount
   - profit
   - record_source

## 3. Диаграмма структуры Data Vault

```
                    ┌─────────────────────┐
                    │   HUB_CUSTOMER      │
                    ├─────────────────────┤
                    │ hub_customer_id (PK)│◄──────┐
                    │ customer_id         │       │
                    │ load_date           │       │
                    │ record_source       │       │
                    └─────────────────────┘       │
                            │                     │
                            │                     │
                            ▼                     │
          ┌────────────────────────────┐          │
          │     SAT_CUSTOMER           │          │
          ├────────────────────────────┤          │
          │ hub_customer_id (PK, FK)   │          │
          │ load_date (PK)             │          │
          │ hash_diff                  │          │
          │ customer_name              │          │
          │ segment                    │          │
          │ record_source              │          │
          └────────────────────────────┘          │
                            │                     │
                            ▼                     │
          ┌────────────────────────────┐          │
          │  SAT_CUSTOMER_LOCATION     │          │
          ├────────────────────────────┤          │
          │ hub_customer_id (PK, FK)   │          │
          │ load_date (PK)             │          │
          │ hash_diff                  │          │
          │ country, region            │          │
          │ state, city, postal_code   │          │
          │ record_source              │          │
          └────────────────────────────┘          │
                                                  │
                                                  │
    ┌─────────────────────┐            ┌────────────────────────┐
    │    HUB_ORDER        │            │ LINK_ORDER_CUSTOMER    │
    ├─────────────────────┤            ├────────────────────────┤
    │ hub_order_id (PK)   │◄───────────┤link_order_customer_id  │
    │ order_id            │            │ hub_customer_id (FK)   │──┘
    │ load_date           │            │ hub_order_id (FK)      │
    │ record_source       │            │ load_date              │
    └─────────────────────┘            │ record_source          │
            │                          └────────────────────────┘
            │
            ▼
    ┌────────────────────────────┐
    │      SAT_ORDER             │
    ├────────────────────────────┤
    │ hub_order_id (PK, FK)      │
    │ load_date (PK)             │
    │ hash_diff                  │
    │ order_date                 │
    │ ship_date                  │
    │ ship_mode                  │
    │ record_source              │
    └────────────────────────────┘
            │
            │
            ▼
    ┌────────────────────────┐
    │ LINK_ORDER_PRODUCT     │
    ├────────────────────────┤
    │link_order_product_id(PK)│◄──────┐
    │ hub_order_id (FK)      │       │
    │ hub_product_id (FK)    │───┐   │
    │ row_id                 │   │   │
    │ load_date              │   │   │
    │ record_source          │   │   │
    └────────────────────────┘   │   │
            │                    │   │
            ▼                    │   │
    ┌────────────────────────┐   │   │
    │  SAT_ORDER_PRODUCT     │   │   │
    ├────────────────────────┤   │   │
    │link_order_product_id(FK)│───┘   │
    │ load_date (PK)         │       │
    │ hash_diff              │       │
    │ sales                  │       │
    │ quantity               │       │
    │ discount               │       │
    │ profit                 │       │
    │ record_source          │       │
    └────────────────────────┘       │
                                     │
                                     │
                    ┌─────────────────────┐
                    │   HUB_PRODUCT       │
                    ├─────────────────────┤
                    │ hub_product_id (PK) │◄──┘
                    │ product_id          │
                    │ load_date           │
                    │ record_source       │
                    └─────────────────────┘
                            │
                            ▼
                    ┌────────────────────────┐
                    │    SAT_PRODUCT         │
                    ├────────────────────────┤
                    │ hub_product_id (PK, FK)│
                    │ load_date (PK)         │
                    │ hash_diff              │
                    │ product_name           │
                    │ category               │
                    │ sub_category           │
                    │ record_source          │
                    └────────────────────────┘
```

## 4. Ключевые особенности проектирования

### 4.1 Хеш-ключи
- Все первичные ключи реализованы как хеш-значения (MD5) от бизнес-ключей
- Используется нормализация перед хешированием (UPPER + BTRIM + COALESCE)
- Это обеспечивает детерминированность и производительность

### 4.2 Историчность данных
- Сателлиты хранят полную историю изменений атрибутов
- load_date и load_end_date определяют временной интервал действия записи
- hash_diff используется для обнаружения изменений данных

### 4.3 Аудит и отслеживание
- record_source указывает источник данных (SampleSuperstore.csv)
- load_date фиксирует время загрузки данных в хранилище

### 4.4 Разделение сателлитов
- Атрибуты клиента разделены на два сателлита:
  - SAT_CUSTOMER - основные атрибуты (имя, сегмент)
  - SAT_CUSTOMER_LOCATION - географические атрибуты (меняются реже)
- Это соответствует best practices Data Vault 2.0 (разделение по частоте изменений)

### 4.5 Масштабируемость
- Структура позволяет легко добавлять новые источники данных
- Новые атрибуты добавляются в виде новых сателлитов
- Новые связи добавляются в виде новых Link-таблиц

## 5. Преимущества данного подхода

1. **Гибкость** - легко адаптируется к изменениям в бизнес-требованиях
2. **Аудируемость** - полная история всех изменений данных
3. **Параллелизм загрузки** - различные сущности могут загружаться независимо
4. **Интеграция** - простое добавление новых источников данных
5. **Производительность** - оптимизирована для массовых параллельных загрузок в Greenplum
