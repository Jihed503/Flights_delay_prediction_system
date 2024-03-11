from confluent_kafka import Producer
import json
from reviews import *

def delivery_report(err, msg):
    '''
    Prints the delivery status of a message.

    Parameters:
    - err: Error object, if any, or None.
    - msg: The message object.

    Outputs a failure message with the error or a success message with the message's topic and partition.
    '''
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

# Producer configuration
conf = {'bootstrap.servers': 'localhost:9092'}

# Create Producer instance
producer = Producer(conf)

reviews = reviews_scraping()

for row in reviews:
    # Convert the row to a JSON string
    message = json.dumps(row)
    # Send the message to a Kafka topic, with a callback for delivery reports
    producer.produce('reviews_data_topic', value=message, callback=delivery_report)

    # Trigger any available delivery report callbacks from previous produce() calls
    producer.poll(0)

# Wait for any outstanding messages to be delivered and delivery report callbacks to be triggered
producer.flush()