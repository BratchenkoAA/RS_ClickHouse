# Отчет по лабораторной работе
## ClickHouse: Создание таблиц и аналитические запросы

### Вариант 1


## 1. Постановка задачи

Разработать базу данных в ClickHouse для анализа продаж. Создать таблицы с использованием различных движков: MergeTree (с партиционированием), ReplacingMergeTree (для хранения последней версии товаров), SummingMergeTree (для агрегации метрик). Выполнить аналитические запросы и комплексный анализ данных.

**Параметры варианта 1:**
- `sale_id` от: 1001
- `customer_id` диапазон: 100–199
- `product_id` диапазон: 10–29
- Максимальное количество товара: 6
- Минимальная цена: 11.00
- Количество товаров (Зад.3): 6
- Количество дней (Зад.4): 4
- Количество кампаний (Зад.4): 3

---

## 2. Задание 1: Создание базы данных и таблицы продаж

### SQL-код создания таблицы
```sql
CREATE TABLE db_1.sales_var001 (
    sale_id        UInt64,
    sale_timestamp DateTime64(3),
    product_id     UInt32,
    category       String,
    customer_id    UInt64,
    region         String,
    quantity       UInt16,
    unit_price     Decimal(10,2),
    discount_pct   Float32,
    is_online      UInt8,
    ip_address     String
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(sale_timestamp)
ORDER BY (sale_timestamp, customer_id, product_id);
```

### Вставка данных (более 100 строк)
```sql
INSERT INTO db_1.sales_var001 (sale_id, sale_timestamp, product_id, category, customer_id, region, quantity, unit_price, discount_pct, is_online, ip_address) VALUES
(1001, '2024-05-01 10:00:00.000', 10, 'electronics', 100, 'Moscow', 3, 50.00, 0.00, 1, '192.168.1.1'),
(1002, '2024-05-02 11:30:00.000', 11, 'clothing', 101, 'SPB', 2, 30.00, 0.10, 1, '192.168.1.2'),
... (всего 132 строки)
```

---

<img width="1504" height="864" alt="image" src="https://github.com/user-attachments/assets/955380b5-b8eb-408c-ac97-29d93c9dca15" />
<img width="221" height="91" alt="image" src="https://github.com/user-attachments/assets/efdbc090-b233-48c0-bb98-e9aea03cde0b" />

---

## 3. Задание 2: Аналитические запросы

### Запрос 1: Общая выручка по категориям
```sql
SELECT 
    category,
    SUM(quantity * unit_price * (1 - discount_pct)) AS revenue
FROM db_1.sales_var001
GROUP BY category
ORDER BY revenue DESC;
```

---

## 📸 **Сделайте скриншот 2:** Результат запроса 1 (выручка по категориям)

---

### Запрос 2: Топ-3 клиента по количеству покупок
```sql
SELECT 
    customer_id,
    count() AS purchase_count,
    SUM(quantity) AS total_quantity
FROM db_1.sales_var001
GROUP BY customer_id
ORDER BY purchase_count DESC
LIMIT 3;
```

---

## 📸 **Сделайте скриншот 3:** Результат запроса 2 (топ-3 клиента)

---

### Запрос 3: Средний чек по месяцам
```sql
SELECT 
    toYYYYMM(sale_timestamp) AS month,
    AVG(quantity * unit_price) AS avg_bill
FROM db_1.sales_var001
GROUP BY month
ORDER BY month;
```

---

## 📸 **Сделайте скриншот 4:** Результат запроса 3 (средний чек по месяцам)

---

### Запрос 4: Фильтрация по партиции (только июнь 2024)
```sql
SELECT *
FROM db_1.sales_var001
WHERE sale_timestamp >= '2024-06-01' AND sale_timestamp < '2024-07-01';
```

---

## 📸 **Сделайте скриншот 5:** Результат запроса 4 (данные за июнь)

---

## 4. Задание 3: ReplacingMergeTree — справочник товаров

### SQL-код создания таблицы
```sql
CREATE TABLE db_1.products_var001 (
    product_id    UInt32,
    product_name  String,
    category      String,
    supplier      String,
    base_price    Decimal(10,2),
    weight_kg     Float32,
    is_available  UInt8,
    updated_at    DateTime,
    version       UInt64
)
ENGINE = ReplacingMergeTree(version)
ORDER BY product_id;
```

### Вставка данных (version = 1)
```sql
INSERT INTO db_1.products_var001 VALUES
(10, 'Ноутбук', 'electronics', 'TechCorp', 50000.00, 2.5, 1, now(), 1),
(11, 'Футболка', 'clothing', 'FashionInc', 1500.00, 0.2, 1, now(), 1),
(12, 'Книга', 'books', 'BookPub', 800.00, 0.5, 1, now(), 1),
(13, 'Смартфон', 'electronics', 'TechCorp', 30000.00, 0.3, 1, now(), 1),
(14, 'Джинсы', 'clothing', 'FashionInc', 3500.00, 0.8, 1, now(), 1),
(15, 'Наушники', 'electronics', 'AudioLab', 5000.00, 0.2, 1, now(), 1);
```

### Обновление 3 товаров (version = 2)
```sql
INSERT INTO db_1.products_var001 VALUES
(10, 'Ноутбук Pro', 'electronics', 'TechCorp', 55000.00, 2.5, 1, now(), 2),
(11, 'Футболка Premium', 'clothing', 'FashionInc', 2500.00, 0.2, 1, now(), 2),
(13, 'Смартфон Pro', 'electronics', 'TechCorp', 35000.00, 0.3, 0, now(), 2);
```

### Проверка — видны обе версии
```sql
SELECT * FROM db_1.products_var001;
```

---

## 📸 **Сделайте скриншот 6:** Результат запроса `SELECT * FROM db_1.products_var001;` (должны быть видны дубликаты)

---

### Проверка — только последние версии
```sql
SELECT * FROM db_1.products_var001 FINAL;
```

---

## 📸 **Сделайте скриншот 7:** Результат запроса `SELECT * FROM db_1.products_var001 FINAL;` (только последние версии)

---

## 5. Задание 4: SummingMergeTree — агрегация метрик

### SQL-код создания таблицы
```sql
CREATE TABLE db_1.daily_metrics_var001 (
    metric_date    Date,
    campaign_id    UInt32,
    channel        String,
    impressions    UInt64,
    clicks         UInt64,
    conversions    UInt32,
    spend_cents    UInt64
)
ENGINE = SummingMergeTree()
ORDER BY (metric_date, campaign_id, channel);
```

### Вставка данных
```sql
INSERT INTO db_1.daily_metrics_var001 VALUES
('2024-06-01', 11, 'google', 1000, 100, 10, 5000),
('2024-06-01', 11, 'yandex', 800, 80, 8, 4000),
('2024-06-02', 11, 'google', 1100, 110, 11, 5500),
('2024-06-02', 11, 'yandex', 850, 85, 9, 4250),
('2024-06-03', 11, 'google', 1200, 120, 12, 6000),
('2024-06-03', 11, 'yandex', 900, 90, 9, 4500),
('2024-06-04', 11, 'google', 1300, 130, 13, 6500),
('2024-06-04', 11, 'yandex', 950, 95, 10, 4750);
```

### Повторные строки для демонстрации суммирования
```sql
INSERT INTO db_1.daily_metrics_var001 VALUES
('2024-06-01', 11, 'google', 100, 5, 1, 500),
('2024-06-02', 11, 'yandex', 50, 3, 0, 250);
```

### Проверка просуммированных данных
```sql
SELECT * FROM db_1.daily_metrics_var001 FINAL;
```

---

## 📸 **Сделайте скриншот 8:** Результат запроса `SELECT * FROM db_1.daily_metrics_var001 FINAL;` (просуммированные значения)

---

### CTR по каналам
```sql
SELECT 
    channel,
    SUM(clicks) AS total_clicks,
    SUM(impressions) AS total_impressions,
    SUM(clicks) / SUM(impressions) AS CTR
FROM db_1.daily_metrics_var001 FINAL
GROUP BY channel;
```

---

## 📸 **Сделайте скриншот 9:** Результат CTR запроса

---

## 6. Задание 5: Комплексный анализ

### 6.1 JOIN между таблицами (топ-5 товаров по выручке)
```sql
SELECT
    p.product_name,
    p.category,
    sum(s.quantity * s.unit_price * (1 - s.discount_pct)) AS revenue
FROM db_1.sales_var001 AS s
INNER JOIN db_1.products_var001 AS p
    ON s.product_id = p.product_id
GROUP BY p.product_name, p.category
ORDER BY revenue DESC
LIMIT 5;
```

---

## 📸 **Сделайте скриншот 10:** Результат JOIN запроса (топ-5 товаров)

---

### 6.2 Запрос с массивом (tags)

```sql
-- Создание таблицы
CREATE TABLE db_1.tags_var001 (
    item_id  UInt32,
    item_name String,
    tags     Array(String)
) ENGINE = MergeTree()
ORDER BY item_id;

-- Вставка данных
INSERT INTO db_1.tags_var001 VALUES
(1, 'Item A', ['sale', 'popular', 'new']),
(2, 'Item B', ['premium', 'limited']),
(3, 'Item C', ['sale', 'clearance']);

-- Запрос
SELECT
    arrayJoin(tags) AS tag,
    count() AS items_count
FROM db_1.tags_var001
GROUP BY tag
ORDER BY items_count DESC;
```

---

## 📸 **Сделайте скриншот 11:** Результат запроса с массивом

---

### 6.3 Контрольная сумма
```sql
SELECT 'sales' AS tbl, count() AS rows, sum(quantity) AS check_sum FROM db_1.sales_var001
UNION ALL
SELECT 'products', count(), sum(toUInt64(product_id)) FROM db_1.products_var001 FINAL
UNION ALL
SELECT 'metrics', count(), sum(clicks) FROM db_1.daily_metrics_var001 FINAL;
```

---

## 📸 **Сделайте скриншот 12:** Результат контрольной суммы

---

## 7. Ответы на вопросы

### 1. Почему LowCardinality(String) эффективнее обычного String для столбца category?

**Ответ:** LowCardinality создает словарь уникальных значений, что уменьшает размер хранения на диске и ускоряет выполнение запросов за счет работы с числовыми индексами вместо строк. Это особенно эффективно для столбцов с небольшим количеством уникальных значений (например, категории товаров).

---

### 2. В чём разница между ORDER BY и PRIMARY KEY в ClickHouse?

**Ответ:** 
- **ORDER BY** — обязательный параметр, определяет физический порядок данных на диске. Данные сортируются по указанным столбцам.
- **PRIMARY KEY** — опциональный параметр, является подмножеством ORDER BY. Используется для создания разреженного индекса, ускоряющего поиск данных. Если PRIMARY KEY не указан, используется ORDER BY.

---

### 3. Когда следует использовать ReplacingMergeTree вместо MergeTree?

**Ответ:** ReplacingMergeTree следует использовать, когда необходимо хранить только последнюю версию строки. Это актуально для:
- Справочников (товары, пользователи), где данные обновляются
- Профилей пользователей
- Ситуаций, где важна актуальность данных, а история изменений не требуется

---

### 4. Почему SummingMergeTree не заменяет GROUP BY в аналитических запросах?

**Ответ:** SummingMergeTree выполняет суммирование только при слиянии частей (merges), что происходит асинхронно в фоне. До момента слияния данные могут быть не просуммированы. Для получения точных результатов необходимо использовать GROUP BY в запросах.

---

### 5. Что произойдёт, если не выполнить OPTIMIZE TABLE FINAL для ReplacingMergeTree?

**Ответ:** Без принудительного слияния дублирующие записи могут оставаться в таблице и быть видны в запросах до момента автоматического слияния, которое происходит в фоне. Это может привести к некорректным результатам, если ожидается уникальность данных.

---

## 8. Вывод

В ходе выполнения лабораторной работы были созданы таблицы `sales_var001`, `products_var001`, `daily_metrics_var001` в базе данных `db_1` с использованием движков MergeTree (с партиционированием по месяцам), ReplacingMergeTree и SummingMergeTree. Выполнена вставка тестовых данных: 132 строки в таблицу продаж, 6 товаров (с обновлением 3 из них), 4 дня метрик.

Реализованы аналитические запросы:
- Общая выручка по категориям
- Топ-3 клиента по количеству покупок
- Средний чек по месяцам
- Фильтрация по партиции

На примере ReplacingMergeTree продемонстрировано хранение только последней версии строки. На примере SummingMergeTree — автоматическое суммирование числовых метрик.

Выполнен комплексный анализ: JOIN между таблицами продаж и товаров, запрос с использованием массива, контрольная сумма.

Полученные навыки позволяют эффективно организовывать хранение и аналитическую обработку данных в ClickHouse.

---
