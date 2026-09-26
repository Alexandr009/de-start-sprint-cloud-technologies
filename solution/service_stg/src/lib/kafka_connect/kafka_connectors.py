import json
from typing import Dict, Optional

from confluent_kafka import OFFSET_BEGINNING, Consumer, KafkaError, KafkaException, Producer


def error_callback(err):
    print('Something went wrong: {}'.format(err))


class KafkaProducer:
    def __init__(self, host: str, port: int, user: str, password: str, topic: str, cert_path: str) -> None:
        params = {
            'bootstrap.servers': f'{host}:{port}',
            'security.protocol': 'SASL_SSL',
            'ssl.ca.location': cert_path,
            'sasl.mechanism': 'SCRAM-SHA-512',
            'sasl.username': user,
            'sasl.password': password,
            'error_cb': error_callback,
        }

        self.topic = topic
        self.p = Producer(params)

    def produce(self, payload: Dict) -> None:
        self.p.produce(self.topic, json.dumps(payload))
        self.p.flush(10)


class KafkaConsumer:
    def __init__(self,
                 host: str,
                 port: int,
                 user: str,
                 password: str,
                 topic: str,
                 group: str,
                 cert_path: str
                 ) -> None:
        params = {
            'bootstrap.servers': f'{host}:{port}',
            'security.protocol': 'SASL_SSL',
            'ssl.ca.location': cert_path,
            'sasl.mechanism': 'SCRAM-SHA-512',
            'sasl.username': user,
            'sasl.password': password,
            'group.id': group,  # '',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
            # librdkafka по умолчанию подкачивает в память до 64 МБ на раздел —
            # под с лимитом 128Mi на длинном топике падал по OOM
            'queued.max.messages.kbytes': 8192,
            'error_cb': error_callback,
            'client.id': 'someclientkey'
        }

        self.topic = topic
        self.c = Consumer(params)
        self.c.subscribe([topic])

    def consume(self, timeout: float = 3.0) -> Optional[Dict]:
        msg = self.c.poll(timeout=timeout)
        if not msg:
            return None
        if msg.error():
            raise Exception(msg.error())
        val = msg.value().decode()
        return json.loads(val)

    def commit(self) -> None:
        # автокоммит выключен: фиксируем позицию явно, когда батч обработан.
        # Если с прошлого коммита ничего не прочитано, Kafka ответит _NO_OFFSET — это не ошибка.
        try:
            self.c.commit(asynchronous=False)
        except KafkaException as e:
            if e.args[0].code() != KafkaError._NO_OFFSET:
                raise

    def rollback(self) -> None:
        # после сбоя возвращаем позицию чтения к последнему коммиту, иначе
        # недообработанные сообщения этого батча до перезапуска больше не прочитаются
        for tp in self.c.committed(self.c.assignment(), timeout=10):
            if tp.offset < 0:
                # коммитов ещё не было — как и auto.offset.reset, читаем с начала
                tp.offset = OFFSET_BEGINNING
            self.c.seek(tp)
