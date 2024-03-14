from confluent_kafka import Consumer, KafkaException, KafkaError
import json

# Consumer configuration
conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'my_group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': True
}

# Create Consumer instance
consumer = Consumer(conf)

# Subscribe to topic
consumer.subscribe(['flights_data_topic'])

# Process messages
try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # End of partition event
                print('%% %s [%d] reached end at offset %d\n' %
                      (msg.topic(), msg.partition(), msg.offset()))
            elif msg.error():
                raise KafkaException(msg.error())
        else:
            # Proper message
            data = json.loads(msg.value().decode('utf-8'))
            # Write the data to a CSV file
            with open('../data/history/flights_kafka.csv', 'a', encoding="utf-8") as file:
                file.write(data)
except KeyboardInterrupt:
    pass
finally:
    # Close down consumer to commit final offsets.
    consumer.close()
