from confluent_kafka import Producer
import socket


def main():
    try:
        conf = {
            'bootstrap.servers': "kafka-1:9092",
            'client.id': socket.gethostname()
        }

        producer = Producer(conf)

        with open('./src/tweets.csv') as f:
            header = f.readline()  # pass first line

            for i in range(10):
                msg = f.readline()
                producer.produce('first_topic', value=msg)

    except Exception as e:
        print(e)


if __name__ == '__main__':
    main()






