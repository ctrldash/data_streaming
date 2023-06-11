import sys
import logging
from confluent_kafka import Consumer, KafkaException, KafkaError
from prometheus_client import start_http_server, Gauge, Summary
from time import sleep, time


logger = logging.getLogger('consumer')
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(asctime)-15s %(levelname)-8s %(message)s'))
logger.addHandler(handler)


metric = Gauge('custom_request_duration', 'CUSTOM Request duration in seconds')
metric2 = Gauge('custom_memory', 'CUSTOM MEMORY')


def msg_process(msg):
    logger.info(msg.value())
    logger.info(msg.timestamp())

    sleep(1)

    current_timestamp = round(time() * 1000)

    value = int(current_timestamp) - int(msg.timestamp()[1])

    logger.info(f"Current timestamp: {int(current_timestamp)}")
    logger.info(f"Kafka timestamp: {int(msg.timestamp()[1])}")

    logger.info(value)
    metric.set(value)

    res = len(str(msg.value()).encode('utf-8'))

    metric2.set(res)


def basic_consume_loop(consumer, topics):
    try:
        consumer.subscribe(topics)
        # out = open("./src/results.csv", "a")

        while running:
            msg = consumer.poll(timeout=1.0)
            if msg is None: continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                     (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                msg_process(msg)
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()


def shutdown():
    running = False


if __name__ == '__main__':
    start_http_server(8000)

    conf = {'bootstrap.servers': 'kafka-1:9092',
        'group.id': "super3",
        'enable.auto.commit': False,
        'auto.offset.reset': 'earliest'}

    consumer = Consumer(conf)
    running = True
    basic_consume_loop(consumer, ["first_topic"])
    shutdown()
