# sprint-9-project

Потоковое DWH для сервиса доставки еды: три микросервиса в Kubernetes читают события заказов из Kafka,
обогащают их справочниками из Valkey (Redis) и слой за слоем наполняют хранилище в PostgreSQL.

## Архитектура

```
order-service_orders ─▶ STG-service ─▶ stg-service-orders ─▶ DDS-service ─▶ dds-service-orders ─▶ CDM-service
   (Kafka, источник)        │  ▲                                   │                                   │
                            │  └── Valkey: users, restaurants       │                                   │
                            ▼                                       ▼                                   ▼
                     stg.order_events                    dds (Data Vault)          cdm.user_product_counters
                                                                                   cdm.user_category_counters
```

| Сервис | Вход | Что делает | Выход |
|---|---|---|---|
| `service_stg` | `order-service_orders` | сохраняет событие как есть в `stg.order_events` (upsert по `object_id`), обогащает заказ пользователем, рестораном и категориями блюд из Valkey | `stg-service-orders` |
| `service_dds` | `stg-service-orders` | раскладывает заказ по Data Vault: 5 хабов, 4 линка, 5 сателлитов | `dds-service-orders` |
| `service_cdm` | `dds-service-orders` | увеличивает счётчики заказов пользователя по блюдам и категориям (только `CLOSED`-заказы) | витрины `cdm.*` |

DDL всех слоёв — [`solution/sql/ddl.sql`](solution/sql/ddl.sql), идемпотентен.

## Container Registry

Реестр: **`cr.yandex/crp9vlq26b21d42l1tfg`** (Yandex Container Registry `de-registry`, публичный pull).

| Сервис | Образ |
|---|---|
| STG | `cr.yandex/crp9vlq26b21d42l1tfg/stg_service:v2026-09-26-r2` |
| DDS | `cr.yandex/crp9vlq26b21d42l1tfg/dds_service:v2026-09-26-r2` |
| CDM | `cr.yandex/crp9vlq26b21d42l1tfg/cdm_service:v2026-09-26-r2` |

## Контракты сообщений

**STG → DDS** (`stg-service-orders`):

```json
{
  "object_id": 322519,
  "object_type": "order",
  "payload": {
    "id": 322519, "date": "2022-11-19 16:06:36", "cost": 300, "payment": 300, "status": "CLOSED",
    "restaurant": {"id": "626a81cfefa404208fe9abae", "name": "Кофейня №1"},
    "user": {"id": "626a81ce9a8cd1920641e296", "name": "Котова Ольга Вениаминовна", "login": "..."},
    "products": [
      {"id": "6276e8cd0cf48b4cded00878", "price": 180, "quantity": 1,
       "name": "РОЛЛ С ТОФУ И ВЯЛЕНЫМИ ТОМАТАМИ", "category": "Выпечка"}
    ]
  }
}
```

**DDS → CDM** (`dds-service-orders`) — идентификаторы уже в виде ключей хабов DDS:

```json
{
  "object_id": 322519,
  "object_type": "order",
  "payload": {
    "id": 322519, "date": "2022-11-19 16:06:36", "status": "CLOSED",
    "user": {"id": "<h_user_pk>"},
    "products": [
      {"id": "<h_product_pk>", "name": "РОЛЛ С ТОФУ И ВЯЛЕНЫМИ ТОМАТАМИ",
       "category": {"id": "<h_category_pk>", "name": "Выпечка"}}
    ]
  }
}
```

## Идемпотентность

Все сервисы коммитят offset в Kafka только после обработки всего батча; при сбое позиция чтения
откатывается к последнему коммиту и сообщения перечитываются. Поэтому каждая запись безопасна к повтору:

- **STG** — `INSERT … ON CONFLICT (object_id) DO UPDATE`.
- **DDS** — ключи хабов и линков детерминированы (`uuid5` от бизнес-ключа), вставка `ON CONFLICT DO NOTHING`;
  в сателлит новая версия пишется, только если `hashdiff` отличается от последней загруженной.
  Заказ пишется в одной транзакции.
- **CDM** — инкремент счётчика не идемпотентен, поэтому учтённые заказы отмечаются в
  `cdm.srv_processed_orders`; отметка и инкременты — в одной транзакции, повторный заказ отсекается.

## Запуск

Локально (из `solution/`, параметры подключения — в файле `.env`, он в `.gitignore`):

```bash
docker compose up -d --build
```

Релиз в Kubernetes (из каталога сервиса). Пароли в `values.yaml` не хранятся — передаются отдельным файлом:

```bash
docker build --provenance=false --sbom=false . -t cr.yandex/crp9vlq26b21d42l1tfg/dds_service:<tag>
docker push cr.yandex/crp9vlq26b21d42l1tfg/dds_service:<tag>
helm upgrade --install --atomic dds-service app -f <secrets.yaml> -n <namespace>
```

`secrets.yaml`:

```yaml
config:
  KAFKA_CONSUMER_PASSWORD: "..."
  PG_WAREHOUSE_PASSWORD: "..."
  REDIS_PASSWORD: "..."   # только для STG
```
