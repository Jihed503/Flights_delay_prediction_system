from confluent_kafka import Consumer, KafkaException, KafkaError
import json
from hdfs import InsecureClient

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
consumer.subscribe(['weather_data_topic'])

# Process messages
try:
    print("Waiting for messages ...")
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
            with open('./data/history/weather.csv', 'a', encoding="utf-8") as file:
                file.write(','.join(data) + '\n')
            
            
            # Write the data to HDFS
            #hdfs_client = InsecureClient('http://localhost:9870', user='JIHED')

            #with hdfs_client.write('data/weather_data.csv', append=True) as writer:
            #    json.dump(data, writer)
            #    writer.write(','.join(data) + '\n')

           

            #hdfs_client.write('data/weather_data.csv', data=json.dumps(','.join(data) + '\n'), encoding='utf-8', append=True)
            

except KeyboardInterrupt:
    exit()
finally:
    # Close down consumer to commit final offsets.
    consumer.close()
