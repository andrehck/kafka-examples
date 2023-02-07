from confluent_kafka import Consumer
import socket, json

conf = {'bootstrap.servers': "localhost:9092",
        'group.id': "ExamplePython"}

consumer = Consumer(conf)

running = True

class Order:
    def _init_(self, id, name, valor, email):
        self.id = id
        self.name = name
        self.valor = valor
        self.email = email

if __name__ == "__main__":
    consumer.subscribe(["ECOMMERCE_NEW_ORDER"])
    while running:
        msg = consumer.poll(10)
        if msg is None: 
            continue
        if msg.error():
            print("Consumer error: {}".format(msg.error()))
            continue
        else: 
            print('Received message: {}'.format(msg.value().decode('utf-8')))
            orderObject=msg.value().decode('utf-8')
            orderObjectToJson= json.loads(orderObject)

consumer.close()
