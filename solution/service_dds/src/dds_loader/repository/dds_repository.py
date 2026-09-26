import uuid
from datetime import datetime
from typing import Any, Dict, List

from lib.pg import PgConnect
from psycopg import Cursor, sql
from pydantic import BaseModel


# ---------- входное сообщение от STG-сервиса (payload) ----------

class User(BaseModel):
    id: str
    name: str
    login: str


class Restaurant(BaseModel):
    id: str
    name: str


class Product(BaseModel):
    id: str
    price: float
    quantity: int
    name: str
    category: str


class Order(BaseModel):
    id: int
    date: datetime
    cost: float
    payment: float
    status: str
    restaurant: Restaurant
    user: User
    products: List[Product]


# ---------- ключи Data Vault ----------

def make_key(*parts: Any) -> uuid.UUID:
    # детерминированный UUID: один и тот же бизнес-ключ всегда даёт один и тот же ключ,
    # поэтому повторная загрузка не порождает новых записей
    return uuid.uuid5(uuid.NAMESPACE_OID, '|'.join(str(p) for p in parts))


class OrderKeys:
    """Ключи хабов заказа — нужны и для записи в DDS, и для выходного сообщения."""

    def __init__(self, order: Order) -> None:
        self.order = make_key(order.id)
        self.user = make_key(order.user.id)
        self.restaurant = make_key(order.restaurant.id)
        self.products = {p.id: make_key(p.id) for p in order.products}
        # у категории нет id — ключ вычисляется из названия
        self.categories = {p.category: make_key(p.category) for p in order.products}


class DdsRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db

    def order_save(self, order: Order, keys: OrderKeys, load_dt: datetime, load_src: str) -> None:
        # весь заказ пишется в одной транзакции: либо целиком, либо никак.
        # pipeline отправляет запросы пачкой, не дожидаясь ответа на каждый
        with self._db.connection() as conn:
            with conn.pipeline():
                with conn.cursor() as cur:
                    self._save(cur, order, keys, {'load_dt': load_dt, 'load_src': load_src})

    def _save(self, cur: Cursor, order: Order, keys: OrderKeys, meta: Dict[str, Any]) -> None:
        # хабы
        self._hub(cur, 'h_order', {'h_order_pk': keys.order, 'order_id': order.id, 'order_dt': order.date}, meta)
        self._hub(cur, 'h_user', {'h_user_pk': keys.user, 'user_id': order.user.id}, meta)
        self._hub(cur, 'h_restaurant', {'h_restaurant_pk': keys.restaurant, 'restaurant_id': order.restaurant.id}, meta)
        for name, pk in keys.categories.items():
            self._hub(cur, 'h_category', {'h_category_pk': pk, 'category_name': name}, meta)
        for product_id, pk in keys.products.items():
            self._hub(cur, 'h_product', {'h_product_pk': pk, 'product_id': product_id}, meta)

        # линки
        self._link(cur, 'l_order_user', 'h_order_pk', keys.order, 'h_user_pk', keys.user, meta)
        for p in order.products:
            product_pk = keys.products[p.id]
            self._link(cur, 'l_order_product', 'h_order_pk', keys.order, 'h_product_pk', product_pk, meta)
            self._link(cur, 'l_product_restaurant', 'h_product_pk', product_pk, 'h_restaurant_pk', keys.restaurant, meta)
            self._link(cur, 'l_product_category', 'h_product_pk', product_pk,
                       'h_category_pk', keys.categories[p.category], meta)

        # сателлиты
        self._satellite(cur, 's_user_names', 'h_user_pk', keys.user,
                        {'username': order.user.name, 'userlogin': order.user.login}, meta)
        self._satellite(cur, 's_restaurant_names', 'h_restaurant_pk', keys.restaurant,
                        {'name': order.restaurant.name}, meta)
        for p in order.products:
            self._satellite(cur, 's_product_names', 'h_product_pk', keys.products[p.id], {'name': p.name}, meta)
        self._satellite(cur, 's_order_cost', 'h_order_pk', keys.order,
                        {'cost': order.cost, 'payment': order.payment}, meta)
        self._satellite(cur, 's_order_status', 'h_order_pk', keys.order, {'status': order.status}, meta)

    def _hub(self, cur: Cursor, table: str, row: Dict[str, Any], meta: Dict[str, Any]) -> None:
        # первое поле row — первичный ключ хаба; объект уже в хабе — ничего не делаем
        row = {**row, **meta}
        pk_col = next(iter(row))
        cur.execute(
            sql.SQL("INSERT INTO dds.{table} ({cols}) VALUES ({vals}) ON CONFLICT ({pk}) DO NOTHING").format(
                table=sql.Identifier(table),
                cols=sql.SQL(', ').join(map(sql.Identifier, row)),
                vals=sql.SQL(', ').join(map(sql.Placeholder, row)),
                pk=sql.Identifier(pk_col)
            ),
            row
        )

    def _link(self, cur: Cursor, table: str,
              hub1_col: str, hub1_pk: uuid.UUID,
              hub2_col: str, hub2_pk: uuid.UUID,
              meta: Dict[str, Any]) -> None:
        row = {'hk_pk': make_key(hub1_pk, hub2_pk), 'hub1': hub1_pk, 'hub2': hub2_pk, **meta}
        cur.execute(
            sql.SQL("""
                INSERT INTO dds.{table} ({hk}, {hub1}, {hub2}, load_dt, load_src)
                VALUES (%(hk_pk)s, %(hub1)s, %(hub2)s, %(load_dt)s, %(load_src)s)
                ON CONFLICT ({hk}) DO NOTHING
            """).format(
                table=sql.Identifier(table),
                hk=sql.Identifier(f'hk_{table[2:]}_pk'),
                hub1=sql.Identifier(hub1_col),
                hub2=sql.Identifier(hub2_col)
            ),
            row
        )

    def _satellite(self, cur: Cursor, table: str,
                   hub_col: str, hub_pk: uuid.UUID,
                   attrs: Dict[str, Any], meta: Dict[str, Any]) -> None:
        # новая версия пишется, только если атрибуты отличаются от последней загруженной:
        # повторное сообщение с теми же данными не создаёт дубль в истории
        hashdiff_col = f'hk_{table[2:]}_hashdiff'
        row = {**attrs, 'hub_pk': hub_pk, 'hashdiff': make_key(hub_pk, *attrs.values()), **meta}
        cur.execute(
            sql.SQL("""
                INSERT INTO dds.{table} ({hub_col}, {attr_cols}, load_dt, load_src, {hashdiff_col})
                SELECT %(hub_pk)s, {attr_vals}, %(load_dt)s, %(load_src)s, %(hashdiff)s
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM (
                        SELECT {hashdiff_col} AS hashdiff
                        FROM dds.{table}
                        WHERE {hub_col} = %(hub_pk)s
                        ORDER BY load_dt DESC
                        LIMIT 1
                    ) AS last_version
                    WHERE last_version.hashdiff = %(hashdiff)s
                )
                ON CONFLICT ({hub_col}, load_dt) DO NOTHING
            """).format(
                table=sql.Identifier(table),
                hub_col=sql.Identifier(hub_col),
                attr_cols=sql.SQL(', ').join(map(sql.Identifier, attrs)),
                attr_vals=sql.SQL(', ').join(map(sql.Placeholder, attrs)),
                hashdiff_col=sql.Identifier(hashdiff_col)
            ),
            row
        )
