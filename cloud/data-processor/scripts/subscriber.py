import pika
import time
import json

def connect_to_rabbitmq(ip, port=5672, attempts=10, socket_timeout=60):
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
            # Connect to RabbitMQ service
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=ip,
                                        port=port,
                                        socket_timeout=socket_timeout)
                                        )

            # If successful
            print(f"({ip}) Connected to broker", flush=True)
            # Save channel to global
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
    # Subscribe to content
    # Call on_message on recieving message
    cback = Callback(channel)
    consumer_tag = channel.basic_consume(
        queue=queue,
        auto_ack=True,
        on_message_callback = cback.on_message
    )
    # Save consumer tag
    cback.consumer_tag = consumer_tag
    # Start loop to comsume messages
    channel.start_consuming()
    return cback

## CALLBACK FOR MESSAGE RECIEVING
class Callback:
    '''
    Callback class which is used to collect received messages
    This is required since a callback function is required with pika
    This doesn't allow custom parameters to extract information from callback function
    '''
    def __init__(self, channel):
        '''
        Initialises variables
        Takes channel to close waiting loop
        '''
        self.data = []
        self.channel = channel
        self.consumer_tag = None
        
    def on_message(self, ch, method, properties, body):
        '''
        Function called when message is received from subscription
        '''
        str_msg = str(json.loads(body))
        print(f"Message recieved from data-preprocessor: {str_msg}", flush=True)

        # Check for finished msg
        if str_msg == "finished":
            # Stop consuming messages
            self.channel.basic_cancel(self.consumer_tag)
            print("Recieved finished token... Stopping consuming", flush=True)
            
        else:
            self.data.append(str_msg)
            


# PUBLISH TO SEND HEARTBEAT TO DATA-PREPROCESSOR
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
                        body=msg,
                        # Mark message to be retained
                        properties=pika.BasicProperties(
                         delivery_mode = pika.spec.PERSISTENT_DELIVERY_MODE
                         ))
    

