-- DDL хранилища sprint9dwh: слои STG, DDS, CDM.
-- Все команды идемпотентны — файл можно применять повторно.

-- ============================== STG ==============================
CREATE SCHEMA IF NOT EXISTS stg;

-- Сырые события заказов из Kafka: payload сохраняется как пришёл.
-- Данные из Redis в STG не пишутся — ими только обогащается сообщение.
CREATE TABLE IF NOT EXISTS stg.order_events (
    id          int       NOT NULL GENERATED ALWAYS AS IDENTITY,
    object_id   int       NOT NULL,
    object_type varchar   NOT NULL,
    sent_dttm   timestamp NOT NULL,
    payload     json      NOT NULL,
    CONSTRAINT order_events_pk PRIMARY KEY (id),
    -- повторно пришедшее событие не создаёт дубль: сервис пишет через ON CONFLICT
    CONSTRAINT order_events_object_id_uindex UNIQUE (object_id)
);

-- ============================== DDS ==============================
-- Data Vault. Ключ хаба h_<object>_pk — UUID, вычисляемый из бизнес-ключа
-- (uuid5 от id объекта), поэтому повторная загрузка даёт тот же ключ.
-- load_dt — UTC без таймзоны, load_src — 'orders-system-kafka'.
CREATE SCHEMA IF NOT EXISTS dds;

-- ------------------------------ хабы ------------------------------
CREATE TABLE IF NOT EXISTS dds.h_user (
    h_user_pk uuid      NOT NULL,
    user_id   varchar   NOT NULL,
    load_dt   timestamp NOT NULL,
    load_src  varchar   NOT NULL,
    CONSTRAINT h_user_pk PRIMARY KEY (h_user_pk)
);

CREATE TABLE IF NOT EXISTS dds.h_product (
    h_product_pk uuid      NOT NULL,
    product_id   varchar   NOT NULL,
    load_dt      timestamp NOT NULL,
    load_src     varchar   NOT NULL,
    CONSTRAINT h_product_pk PRIMARY KEY (h_product_pk)
);

-- у категории в источнике нет id: бизнес-ключ — название,
-- h_category_pk вычисляется из category_name
CREATE TABLE IF NOT EXISTS dds.h_category (
    h_category_pk uuid      NOT NULL,
    category_name varchar   NOT NULL,
    load_dt       timestamp NOT NULL,
    load_src      varchar   NOT NULL,
    CONSTRAINT h_category_pk PRIMARY KEY (h_category_pk)
);

CREATE TABLE IF NOT EXISTS dds.h_restaurant (
    h_restaurant_pk uuid      NOT NULL,
    restaurant_id   varchar   NOT NULL,
    load_dt         timestamp NOT NULL,
    load_src        varchar   NOT NULL,
    CONSTRAINT h_restaurant_pk PRIMARY KEY (h_restaurant_pk)
);

-- order_id — object_id события (целое); order_dt — время заказа (payload.date),
-- неизменный обязательный атрибут, поэтому хранится в хабе, а не в сателлите
CREATE TABLE IF NOT EXISTS dds.h_order (
    h_order_pk uuid      NOT NULL,
    order_id   int       NOT NULL,
    order_dt   timestamp NOT NULL,
    load_dt    timestamp NOT NULL,
    load_src   varchar   NOT NULL,
    CONSTRAINT h_order_pk PRIMARY KEY (h_order_pk)
);

-- ------------------------------ линки -----------------------------
-- hk_<link>_pk — UUID, вычисляемый из пары ключей хабов;
-- ключи хабов — внешние ключи на соответствующие хабы.
CREATE TABLE IF NOT EXISTS dds.l_order_product (
    hk_order_product_pk uuid      NOT NULL,
    h_order_pk          uuid      NOT NULL,
    h_product_pk        uuid      NOT NULL,
    load_dt             timestamp NOT NULL,
    load_src            varchar   NOT NULL,
    CONSTRAINT l_order_product_pk PRIMARY KEY (hk_order_product_pk),
    CONSTRAINT l_order_product_h_order_fk
        FOREIGN KEY (h_order_pk) REFERENCES dds.h_order (h_order_pk),
    CONSTRAINT l_order_product_h_product_fk
        FOREIGN KEY (h_product_pk) REFERENCES dds.h_product (h_product_pk)
);

CREATE TABLE IF NOT EXISTS dds.l_product_restaurant (
    hk_product_restaurant_pk uuid      NOT NULL,
    h_product_pk             uuid      NOT NULL,
    h_restaurant_pk          uuid      NOT NULL,
    load_dt                  timestamp NOT NULL,
    load_src                 varchar   NOT NULL,
    CONSTRAINT l_product_restaurant_pk PRIMARY KEY (hk_product_restaurant_pk),
    CONSTRAINT l_product_restaurant_h_product_fk
        FOREIGN KEY (h_product_pk) REFERENCES dds.h_product (h_product_pk),
    CONSTRAINT l_product_restaurant_h_restaurant_fk
        FOREIGN KEY (h_restaurant_pk) REFERENCES dds.h_restaurant (h_restaurant_pk)
);

CREATE TABLE IF NOT EXISTS dds.l_product_category (
    hk_product_category_pk uuid      NOT NULL,
    h_product_pk           uuid      NOT NULL,
    h_category_pk          uuid      NOT NULL,
    load_dt                timestamp NOT NULL,
    load_src               varchar   NOT NULL,
    CONSTRAINT l_product_category_pk PRIMARY KEY (hk_product_category_pk),
    CONSTRAINT l_product_category_h_product_fk
        FOREIGN KEY (h_product_pk) REFERENCES dds.h_product (h_product_pk),
    CONSTRAINT l_product_category_h_category_fk
        FOREIGN KEY (h_category_pk) REFERENCES dds.h_category (h_category_pk)
);

CREATE TABLE IF NOT EXISTS dds.l_order_user (
    hk_order_user_pk uuid      NOT NULL,
    h_order_pk       uuid      NOT NULL,
    h_user_pk        uuid      NOT NULL,
    load_dt          timestamp NOT NULL,
    load_src         varchar   NOT NULL,
    CONSTRAINT l_order_user_pk PRIMARY KEY (hk_order_user_pk),
    CONSTRAINT l_order_user_h_order_fk
        FOREIGN KEY (h_order_pk) REFERENCES dds.h_order (h_order_pk),
    CONSTRAINT l_order_user_h_user_fk
        FOREIGN KEY (h_user_pk) REFERENCES dds.h_user (h_user_pk)
);

-- ---------------------------- сателлиты ---------------------------
-- PK — (ключ хаба, load_dt): каждая версия атрибутов — отдельная строка.
-- hk_<sat>_hashdiff — UUID от всех атрибутов строки, чтобы понять,
-- изменились ли данные с прошлой загрузки.
CREATE TABLE IF NOT EXISTS dds.s_user_names (
    h_user_pk              uuid      NOT NULL,
    username               varchar   NOT NULL,
    userlogin              varchar   NOT NULL,
    load_dt                timestamp NOT NULL,
    load_src               varchar   NOT NULL,
    hk_user_names_hashdiff uuid      NOT NULL,
    CONSTRAINT s_user_names_pk PRIMARY KEY (h_user_pk, load_dt),
    CONSTRAINT s_user_names_h_user_fk
        FOREIGN KEY (h_user_pk) REFERENCES dds.h_user (h_user_pk)
);

CREATE TABLE IF NOT EXISTS dds.s_product_names (
    h_product_pk              uuid      NOT NULL,
    name                      varchar   NOT NULL,
    load_dt                   timestamp NOT NULL,
    load_src                  varchar   NOT NULL,
    hk_product_names_hashdiff uuid      NOT NULL,
    CONSTRAINT s_product_names_pk PRIMARY KEY (h_product_pk, load_dt),
    CONSTRAINT s_product_names_h_product_fk
        FOREIGN KEY (h_product_pk) REFERENCES dds.h_product (h_product_pk)
);

CREATE TABLE IF NOT EXISTS dds.s_restaurant_names (
    h_restaurant_pk              uuid      NOT NULL,
    name                         varchar   NOT NULL,
    load_dt                      timestamp NOT NULL,
    load_src                     varchar   NOT NULL,
    hk_restaurant_names_hashdiff uuid      NOT NULL,
    CONSTRAINT s_restaurant_names_pk PRIMARY KEY (h_restaurant_pk, load_dt),
    CONSTRAINT s_restaurant_names_h_restaurant_fk
        FOREIGN KEY (h_restaurant_pk) REFERENCES dds.h_restaurant (h_restaurant_pk)
);

-- стоимость и статус заказа — в разных сателлитах: меняются независимо
CREATE TABLE IF NOT EXISTS dds.s_order_cost (
    h_order_pk             uuid           NOT NULL,
    cost                   decimal(19, 5) NOT NULL CHECK (cost >= 0),
    payment                decimal(19, 5) NOT NULL CHECK (payment >= 0),
    load_dt                timestamp      NOT NULL,
    load_src               varchar        NOT NULL,
    hk_order_cost_hashdiff uuid           NOT NULL,
    CONSTRAINT s_order_cost_pk PRIMARY KEY (h_order_pk, load_dt),
    CONSTRAINT s_order_cost_h_order_fk
        FOREIGN KEY (h_order_pk) REFERENCES dds.h_order (h_order_pk)
);

-- status — final_status из события (например, CLOSED)
CREATE TABLE IF NOT EXISTS dds.s_order_status (
    h_order_pk               uuid      NOT NULL,
    status                   varchar   NOT NULL,
    load_dt                  timestamp NOT NULL,
    load_src                 varchar   NOT NULL,
    hk_order_status_hashdiff uuid      NOT NULL,
    CONSTRAINT s_order_status_pk PRIMARY KEY (h_order_pk, load_dt),
    CONSTRAINT s_order_status_h_order_fk
        FOREIGN KEY (h_order_pk) REFERENCES dds.h_order (h_order_pk)
);

-- ============================== CDM ==============================
CREATE SCHEMA IF NOT EXISTS cdm;

-- Счётчик заказов пользователя по блюдам.
-- Ключи — UUID хабов из DDS; ресторан не нужен: одно и то же блюдо из разных
-- ресторанов попадает в один счётчик.
CREATE TABLE IF NOT EXISTS cdm.user_product_counters (
    id           int     NOT NULL GENERATED ALWAYS AS IDENTITY,
    user_id      uuid    NOT NULL,
    product_id   uuid    NOT NULL,
    product_name varchar NOT NULL,
    order_cnt    int     NOT NULL CHECK (order_cnt >= 0),
    CONSTRAINT user_product_counters_pk PRIMARY KEY (id)
);

-- пара «пользователь — блюдо» уникальна; по ней же сервис обновляет счётчик
CREATE UNIQUE INDEX IF NOT EXISTS user_product_counters_user_product_uidx
    ON cdm.user_product_counters (user_id, product_id);

-- Счётчик заказов пользователя по категориям блюд.
-- У категорий в источнике нет id: category_id — UUID хаба категории из DDS,
-- вычисленный из названия категории.
CREATE TABLE IF NOT EXISTS cdm.user_category_counters (
    id            int     NOT NULL GENERATED ALWAYS AS IDENTITY,
    user_id       uuid    NOT NULL,
    category_id   uuid    NOT NULL,
    category_name varchar NOT NULL,
    order_cnt     int     NOT NULL CHECK (order_cnt >= 0),
    CONSTRAINT user_category_counters_pk PRIMARY KEY (id)
);

CREATE UNIQUE INDEX IF NOT EXISTS user_category_counters_user_category_uidx
    ON cdm.user_category_counters (user_id, category_id);

-- Служебная таблица CDM-сервиса: заказы, уже учтённые в счётчиках.
-- Инкремент счётчика не идемпотентен, поэтому повторно пришедший заказ
-- (перечитка Kafka, повтор из DDS) отсекается по order_id.
CREATE TABLE IF NOT EXISTS cdm.srv_processed_orders (
    order_id     int       NOT NULL,
    processed_dt timestamp NOT NULL DEFAULT (now() AT TIME ZONE 'utc'),
    CONSTRAINT srv_processed_orders_pk PRIMARY KEY (order_id)
);
