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
            st = f.read()

        for idx, i in enumerate(st.split('\n')):
            if idx < 10:
                print(i)
                producer.produce('first_topic', value=i)
            else:
                break
    except Exception as e:
        print(e)


if __name__ == '__main__':
    main()






