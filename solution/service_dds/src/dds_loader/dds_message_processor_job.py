from datetime import datetime
from logging import Logger
from typing import Dict

from dds_loader.repository import DdsRepository, Order, OrderKeys
from lib.kafka_connect import KafkaConsumer, KafkaProducer


class DdsMessageProcessor:
    LOAD_SRC = 'orders-system-kafka'

    def __init__(self,
                 consumer: KafkaConsumer,
                 producer: KafkaProducer,
                 dds_repository: DdsRepository,
                 batch_size: int,
                 logger: Logger) -> None:
        self._consumer = consumer
        self._producer = producer
        self._dds_repository = dds_repository
        self._batch_size = batch_size
        self._logger = logger

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START")

        try:
            processed = self._process_batch()
        except Exception:
            # батч не закоммичен — возвращаем консьюмер к последнему коммиту,
            # чтобы следующий запуск перечитал эти сообщения
            self._consumer.rollback()
            raise

        # offset фиксируем после того, как весь батч записан и отправлен:
        # при падении сообщения перечитаются, а запись в DDS идемпотентна
        self._consumer.commit()

        self._logger.info(f"{datetime.utcnow()}: FINISH, sent {processed} messages")

    def _process_batch(self) -> int:
        processed = 0
        for _ in range(self._batch_size):
            msg = self._consumer.consume()
            if msg is None:
                break

            if msg.get('object_type') != 'order' or 'payload' not in msg:
                self._logger.info(f"{datetime.utcnow()}: skip non-order message {msg}")
                continue

            order = Order(**msg['payload'])
            keys = OrderKeys(order)

            self._dds_repository.order_save(order, keys, datetime.utcnow(), self.LOAD_SRC)

            self._producer.produce(self._build_output_message(order, keys))
            processed += 1

        return processed

    def _build_output_message(self, order: Order, keys: OrderKeys) -> Dict:
        # CDM-сервису нужны ключи DDS (витрины строятся по ним) и названия;
        # статус — чтобы в счётчики попадали только закрытые заказы
        return {
            'object_id': order.id,
            'object_type': 'order',
            'payload': {
                'id': order.id,
                'date': order.date.strftime('%Y-%m-%d %H:%M:%S'),
                'status': order.status,
                'user': {
                    'id': str(keys.user)
                },
                'products': [
                    {
                        'id': str(keys.products[p.id]),
                        'name': p.name,
                        'category': {
                            'id': str(keys.categories[p.category]),
                            'name': p.category
                        }
                    }
                    for p in order.products
                ]
            }
        }
