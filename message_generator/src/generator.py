from time import sleep
from confluent_kafka import Producer
import socket


def main():
    try:
        sleep(20)
        conf = {
            'bootstrap.servers': "kafka-1:9092",
            'client.id': socket.gethostname()
        }

        producer = Producer(conf)

        with open('./src/tweets.csv') as f:
            st = f.read()

        for idx, i in enumerate(st.split('\n')):
            if idx < 10:
                print(i)
                sleep(1)
                producer.produce('first_topic', value=i)
            else:
                break
    except Exception as e:
        print(e)


if __name__ == '__main__':
    main()






