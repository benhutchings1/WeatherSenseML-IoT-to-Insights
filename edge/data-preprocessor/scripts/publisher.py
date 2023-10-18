import json
from paho.mqtt import client as mqtt_client
import time
import pika

def connect_to_rabbitmq(ip, port=5672, attempts=50, socket_timeout=60):
    '''
    Connects to RabbitMQ MQTT message broker
    https://www.rabbitmq.com/
    Connects with pika.BlockingConnection

    Attributes:
        ip: IP of message broker
        port: Port of message broker
        attempts: Number of attempts to connect
        socket_timeout: Timeout with for connections to broker

    Returns: pika.BlockingConnection

    Raises:
        ConnectionError: if all :attempts: fail
    '''
    connected = False    
    # Make connection attempts
    for i in range(attempts):
        try:    
            # Connect to MQTT service
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=ip,
                                                                            port=port,
                                                                            socket_timeout=socket_timeout))

            # If successful
            print(f"({ip}) Connected to broker", flush=True)

            channel = connection.channel()
            connected = True
            break
        except pika.exceptions.AMQPConnectionError:
            # Sleep for 5 seconds before trying again
            print(f"({ip}) Could not connect, trying again in 5 seconds", flush=True)
            time.sleep(5)
    
    # If connection attempts fail
    if not connected:
        raise ConnectionError(f"Could not connect to {ip}")
    return channel


def publish_to_rabbitmq(channel, queue_name, msg):
    '''
    Publishes to RabbitMQ message broker
    This sends a "heartbeat" message to show data-preprocessor that data-processor is active

    Attributes:
        channel: pika.BlockingConnection to broker
        queue_name: Name of queue to publish to 
        msg: Message payload
            (note) has to be str, int, float, None or bytearray

    Returns: None

    Raises:
        AssertionError: if :msg: is not str, int, float, None or bytearray
    '''
    # Assert parameters are correct types
    assert type(queue_name) == str
    assert type(msg) == str or\
            type(msg) == int or\
            type(msg) == float or\
            type(msg) == None or\
            type(msg) == bytearray

    msg = json.dumps(msg)
    channel.queue_declare(queue=queue_name)
    channel.basic_publish(exchange='',
                        routing_key=queue_name,
                        body=msg)
    

## SUBSCRIBE TO RABBITMQ TO HEAR HEARTBEAT MESSAGE
def rabbitmq_subscribe_to_queue(channel, queue):
    '''
    Subscribes to queue on RabbitMQ broker
    Attributes:
        Channel: pika.BlockingConnection to broker
        queue: Name of queue

    Returns: 
        Callback object to access received messages      
    '''
    # Initialise queue
    channel.queue_declare(queue=queue)

    # Declare callback obj to pass data
    heartbeat_cback = HeartbeatCallback(channel)

    # Subscribe to content
    # Call on_message on recieving message
    heartbeat_cback.consumer_tag = channel.basic_consume(
        queue=queue,
        auto_ack=True,
        on_message_callback = heartbeat_cback.on_heartbeat
    )
    channel.start_consuming()
    

# Callback for hearing heartbeat message
class HeartbeatCallback:
    '''
    Callback class to access heartbeat messages
    Cancels forever loop when heartbeat message is received
    '''
    def __init__(self, channel):
        self.consumer_tag = None
        self.channel = channel

    def on_heartbeat(self, ch, method, properties, body):
        '''
        Callback function which is called when heartbeat message from 
        data-processor is received
        '''
        if json.loads(body) == "ack":
            print("Recieved heartbeat message from data-processor", flush=True)
            # Cancel subscribe to queue and unblock thread
            self.channel.basic_cancel(self.consumer_tag)