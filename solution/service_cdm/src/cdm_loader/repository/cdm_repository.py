from typing import Dict
from uuid import UUID

from lib.pg import PgConnect


class CdmRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db

    def order_counters_apply(self,
                             order_id: int,
                             user_id: UUID,
                             products: Dict[UUID, str],
                             categories: Dict[UUID, str]
                             ) -> bool:
        """Учитывает заказ в счётчиках витрин. Возвращает False, если заказ уже был учтён."""
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                # отметка «заказ учтён» и инкременты — в одной транзакции:
                # повторно пришедший заказ не увеличит счётчики второй раз
                cur.execute(
                    """
                        INSERT INTO cdm.srv_processed_orders (order_id)
                        VALUES (%(order_id)s)
                        ON CONFLICT (order_id) DO NOTHING;
                    """,
                    {'order_id': order_id}
                )
                if cur.rowcount == 0:
                    return False

                for product_id, product_name in products.items():
                    cur.execute(
                        """
                            INSERT INTO cdm.user_product_counters (user_id, product_id, product_name, order_cnt)
                            VALUES (%(user_id)s, %(product_id)s, %(product_name)s, 1)
                            ON CONFLICT (user_id, product_id) DO UPDATE
                            SET
                                order_cnt = user_product_counters.order_cnt + 1,
                                product_name = EXCLUDED.product_name;
                        """,
                        {'user_id': user_id, 'product_id': product_id, 'product_name': product_name}
                    )

                for category_id, category_name in categories.items():
                    cur.execute(
                        """
                            INSERT INTO cdm.user_category_counters (user_id, category_id, category_name, order_cnt)
                            VALUES (%(user_id)s, %(category_id)s, %(category_name)s, 1)
                            ON CONFLICT (user_id, category_id) DO UPDATE
                            SET
                                order_cnt = user_category_counters.order_cnt + 1,
                                category_name = EXCLUDED.category_name;
                        """,
                        {'user_id': user_id, 'category_id': category_id, 'category_name': category_name}
                    )
        return True
