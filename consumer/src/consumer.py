import sys
import logging
from confluent_kafka import Consumer, KafkaException, KafkaError


logger = logging.getLogger('consumer')
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(asctime)-15s %(levelname)-8s %(message)s'))
logger.addHandler(handler)


def msg_process(msg):
    logger.info("I`m here, inside msg_process function")
    logger.info(msg.value())
    logger.info(msg.topic())


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
    conf = {'bootstrap.servers': 'kafka-1:9092',
        'group.id': "super3",
        'enable.auto.commit': False,
        'auto.offset.reset': 'earliest'}

    consumer = Consumer(conf)
    running = True
    basic_consume_loop(consumer, ["first_topic"])
    shutdown()