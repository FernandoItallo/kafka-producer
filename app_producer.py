from kafka import KafkaProducer
from kafka.errors import KafkaError

producer = KafkaProducer(bootstrap_servers=['127.0.0.1:9092'],  value_serializer=lambda v: str(v).encode('utf-8'))


# produce keyed messages to enable hashed partitioning
producer.send('first_topic', value='its  work')
producer.flush()
producer.close(10)
print('Message published successfully.')
