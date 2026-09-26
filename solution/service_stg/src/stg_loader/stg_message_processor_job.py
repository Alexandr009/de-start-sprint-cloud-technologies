import json
from datetime import datetime
from logging import Logger
from typing import Dict, List, Optional

from lib.kafka_connect import KafkaConsumer, KafkaProducer
from lib.redis import RedisClient
from stg_loader.repository import StgRepository


class StgMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 producer: KafkaProducer,
                 redis_client: RedisClient,
                 stg_repository: StgRepository,
                 batch_size: int,
                 logger: Logger) -> None:
        self._consumer = consumer
        self._producer = producer
        self._redis = redis_client
        self._stg_repository = stg_repository
        self._batch_size = batch_size
        self._logger = logger

    # функция, которая будет вызываться по расписанию.
    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START")

        try:
            processed = self._process_batch()
        except Exception:
            # батч не закоммичен — возвращаем консьюмер к последнему коммиту,
            # чтобы следующий запуск перечитал эти сообщения
            self._consumer.rollback()
            raise

        # offset фиксируем только после того, как весь батч записан и отправлен:
        # при падении сообщения перечитаются, а upsert защитит STG от дублей
        self._consumer.commit()

        self._logger.info(f"{datetime.utcnow()}: FINISH, sent {processed} messages")

    def _process_batch(self) -> int:
        processed = 0
        for _ in range(self._batch_size):
            msg = self._consumer.consume()
            # очередь вычитана до конца — заканчиваем батч раньше
            if msg is None:
                break

            # в топике встречаются служебные сообщения без заказа — пропускаем их
            if msg.get('object_type') != 'order' or 'payload' not in msg:
                self._logger.info(f"{datetime.utcnow()}: skip non-order message {msg}")
                continue

            # 1. исходное событие — в STG как есть (upsert по object_id)
            self._stg_repository.order_events_insert(
                msg['object_id'],
                msg['object_type'],
                datetime.fromisoformat(msg['sent_dttm']),
                json.dumps(msg['payload'], ensure_ascii=False)
            )

            # 2. обогащение из Redis и отправка дальше
            output = self._build_output_message(msg)
            if output is None:
                continue
            self._producer.produce(output)
            processed += 1

        return processed

    def _build_output_message(self, msg: Dict) -> Optional[Dict]:
        order = msg['payload']

        user_id = order['user']['id']
        user = self._redis.get(user_id)
        if user is None:
            self._logger.warning(f"order {msg['object_id']}: user {user_id} not found in Redis")
            return None

        restaurant_id = order['restaurant']['id']
        restaurant = self._redis.get(restaurant_id)
        if restaurant is None:
            self._logger.warning(f"order {msg['object_id']}: restaurant {restaurant_id} not found in Redis")
            return None

        products = self._build_products(order['order_items'], restaurant['menu'])
        if products is None:
            self._logger.warning(f"order {msg['object_id']}: product not found in menu of {restaurant_id}")
            return None

        return {
            'object_id': msg['object_id'],
            'object_type': 'order',
            'payload': {
                'id': msg['object_id'],
                'date': order['date'],
                'cost': order['cost'],
                'payment': order['payment'],
                'status': order['final_status'],
                'restaurant': {
                    'id': restaurant_id,
                    'name': restaurant['name']
                },
                'user': {
                    'id': user_id,
                    'name': user['name'],
                    # login нужен DDS для сателлита s_user_names
                    'login': user['login']
                },
                'products': products
            }
        }

    def _build_products(self, order_items: List[Dict], menu: List[Dict]) -> Optional[List[Dict]]:
        # отдельных ключей для блюд в Redis нет: категорию берём из меню ресторана
        menu_by_id = {item['_id']: item for item in menu}

        products = []
        for item in order_items:
            menu_item = menu_by_id.get(item['id'])
            if menu_item is None:
                return None
            products.append({
                'id': item['id'],
                'price': item['price'],
                'quantity': item['quantity'],
                'name': menu_item['name'],
                'category': menu_item['category']
            })
        return products
