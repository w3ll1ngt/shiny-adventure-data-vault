# Проектирование хранилища данных методом Data Vault

## 1. Описание бизнес-контекста

Данное хранилище данных проектируется для системы розничной торговли (e-commerce). 

### Исходные данные

Источником данных является система электронной коммерции со следующими сущностями:

**Customers (Клиенты)**
- customer_id - идентификатор клиента
- first_name - имя
- last_name - фамилия
- email - электронная почта
- phone - телефон
- registration_date - дата регистрации
- address - адрес
- city - город
- country - страна

**Products (Товары)**
- product_id - идентификатор товара
- product_name - название товара
- category - категория
- brand - бренд
- price - цена
- description - описание

**Orders (Заказы)**
- order_id - идентификатор заказа
- customer_id - идентификатор клиента
- order_date - дата заказа
- order_status - статус заказа
- total_amount - общая сумма
- shipping_address - адрес доставки

**Order_Items (Позиции заказа)**
- order_item_id - идентификатор позиции
- order_id - идентификатор заказа
- product_id - идентификатор товара
- quantity - количество
- unit_price - цена за единицу
- discount - скидка

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
   - order_item_sequence (деловой ключ последовательности)
   - load_date
   - record_source

### 2.3 Satellite Tables (Сателлиты)

Сателлиты содержат описательные атрибуты и историю изменений:

1. **SAT_CUSTOMER** - Атрибуты клиента
   - hub_customer_id (PK, FK)
   - load_date (PK)
   - load_end_date
   - hash_diff (хеш для обнаружения изменений)
   - first_name
   - last_name
   - email
   - phone
   - address
   - city
   - country
   - record_source

2. **SAT_PRODUCT** - Атрибуты товара
   - hub_product_id (PK, FK)
   - load_date (PK)
   - load_end_date
   - hash_diff
   - product_name
   - category
   - brand
   - price
   - description
   - record_source

3. **SAT_ORDER** - Атрибуты заказа
   - hub_order_id (PK, FK)
   - load_date (PK)
   - load_end_date
   - hash_diff
   - order_date
   - order_status
   - total_amount
   - shipping_address
   - record_source

4. **SAT_ORDER_PRODUCT** - Атрибуты позиции заказа
   - link_order_product_id (PK, FK)
   - load_date (PK)
   - load_end_date
   - hash_diff
   - quantity
   - unit_price
   - discount
   - record_source

## 3. Диаграмма структуры Data Vault

```
                    ┌─────────────────┐
                    │  HUB_CUSTOMER   │
                    ├─────────────────┤
                    │ hub_customer_id │◄──────┐
                    │ customer_id     │       │
                    │ load_date       │       │
                    │ record_source   │       │
                    └─────────────────┘       │
                            │                 │
                            │                 │
                            ▼                 │
                    ┌─────────────────┐       │
                    │  SAT_CUSTOMER   │       │
                    ├─────────────────┤       │
                    │ hub_customer_id │       │
                    │ load_date       │       │
                    │ hash_diff       │       │
                    │ first_name      │       │
                    │ last_name       │       │
                    │ email           │       │
                    │ ...             │       │
                    └─────────────────┘       │
                                              │
                                              │
    ┌─────────────────┐              ┌──────────────────────┐
    │   HUB_ORDER     │              │ LINK_ORDER_CUSTOMER  │
    ├─────────────────┤              ├──────────────────────┤
    │ hub_order_id    │◄─────────────┤link_order_customer_id│
    │ order_id        │              │ hub_customer_id      │──┘
    │ load_date       │              │ hub_order_id         │
    │ record_source   │              │ load_date            │
    └─────────────────┘              │ record_source        │
            │                        └──────────────────────┘
            │
            ▼
    ┌─────────────────┐
    │   SAT_ORDER     │
    ├─────────────────┤
    │ hub_order_id    │
    │ load_date       │
    │ hash_diff       │
    │ order_date      │
    │ order_status    │
    │ total_amount    │
    │ ...             │
    └─────────────────┘
            │
            │
            ▼
    ┌──────────────────────┐
    │ LINK_ORDER_PRODUCT   │
    ├──────────────────────┤
    │link_order_product_id │◄──────┐
    │ hub_order_id         │       │
    │ hub_product_id       │───┐   │
    │ load_date            │   │   │
    │ record_source        │   │   │
    └──────────────────────┘   │   │
            │                  │   │
            ▼                  │   │
    ┌──────────────────────┐   │   │
    │ SAT_ORDER_PRODUCT    │   │   │
    ├──────────────────────┤   │   │
    │link_order_product_id │───┘   │
    │ load_date            │       │
    │ hash_diff            │       │
    │ quantity             │       │
    │ unit_price           │       │
    │ discount             │       │
    └──────────────────────┘       │
                                   │
                                   │
                    ┌─────────────────┐
                    │  HUB_PRODUCT    │
                    ├─────────────────┤
                    │ hub_product_id  │◄──┘
                    │ product_id      │
                    │ load_date       │
                    │ record_source   │
                    └─────────────────┘
                            │
                            ▼
                    ┌─────────────────┐
                    │  SAT_PRODUCT    │
                    ├─────────────────┤
                    │ hub_product_id  │
                    │ load_date       │
                    │ hash_diff       │
                    │ product_name    │
                    │ category        │
                    │ brand           │
                    │ price           │
                    │ ...             │
                    └─────────────────┘
```

## 4. Ключевые особенности проектирования

### 4.1 Хеш-ключи
- Все первичные ключи реализованы как хеш-значения (MD5) от бизнес-ключей
- Это обеспечивает детерминированность и производительность

### 4.2 Историчность данных
- Сателлиты хранят полную историю изменений атрибутов
- load_date и load_end_date определяют временной интервал действия записи
- hash_diff используется для обнаружения изменений данных

### 4.3 Аудит и отслеживание
- record_source указывает источник данных
- load_date фиксирует время загрузки данных в хранилище

### 4.4 Масштабируемость
- Структура позволяет легко добавлять новые источники данных
- Новые атрибуты добавляются в виде новых сателлитов
- Новые связи добавляются в виде новых Link-таблиц

## 5. Преимущества данного подхода

1. **Гибкость** - легко адаптируется к изменениям в бизнес-требованиях
2. **Аудируемость** - полная история всех изменений данных
3. **Параллелизм загрузки** - различные сущности могут загружаться независимо
4. **Интеграция** - простое добавление новых источников данных
5. **Производительность** - оптимизирована для массовых параллельных загрузок
