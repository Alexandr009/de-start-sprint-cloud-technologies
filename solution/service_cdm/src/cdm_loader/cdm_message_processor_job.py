from datetime import datetime
from logging import Logger
from uuid import UUID

from cdm_loader.repository import CdmRepository
from lib.kafka_connect import KafkaConsumer


class CdmMessageProcessor:
    # в счётчики попадают только выполненные заказы; отменённые не считаются
    COUNTED_STATUS = 'CLOSED'

    def __init__(self,
                 consumer: KafkaConsumer,
                 cdm_repository: CdmRepository,
                 batch_size: int,
                 logger: Logger,
                 ) -> None:
        self._consumer = consumer
        self._cdm_repository = cdm_repository
        self._batch_size = batch_size
        self._logger = logger

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START")

        try:
            counted = self._process_batch()
        except Exception:
            # батч не закоммичен — следующий запуск перечитает эти сообщения;
            # уже учтённые заказы отсечёт cdm.srv_processed_orders
            self._consumer.rollback()
            raise

        self._consumer.commit()

        self._logger.info(f"{datetime.utcnow()}: FINISH, counted {counted} orders")

    def _process_batch(self) -> int:
        counted = 0
        for _ in range(self._batch_size):
            msg = self._consumer.consume()
            if msg is None:
                break

            if msg.get('object_type') != 'order' or 'payload' not in msg:
                self._logger.info(f"{datetime.utcnow()}: skip non-order message {msg}")
                continue

            order = msg['payload']
            if order['status'] != self.COUNTED_STATUS:
                continue

            # счётчик — число заказов, поэтому блюдо или категория,
            # встретившиеся в заказе несколько раз, учитываются один раз
            products = {UUID(p['id']): p['name'] for p in order['products']}
            categories = {UUID(p['category']['id']): p['category']['name'] for p in order['products']}

            if self._cdm_repository.order_counters_apply(order['id'], UUID(order['user']['id']),
                                                         products, categories):
                counted += 1

        return counted
