import json
from paho.mqtt import client as mqtt_client
import time
import preprocessing
import publisher

counter = 0

## Connect to data-injector
def connect_to_mqtt(ip, port=1883, attempts=20):
    '''
    Connects to EMQX MQTT message broker
    https://www.emqx.io/

    Attributes:
        ip: IP of message broker
        port: Port of message broker
        attempts: Number of attempts to connect

    Returns: tuple of: 
        EMQX Client blocking connection
        Callback object to access messages (see below)

    Raises:
        ConnectionError: if all :attempts: fail
    '''
    # Create a mqtt client object
    client = mqtt_client.Client()
    # Add callbacks
    client.message_callback_add("check", on_message_ack) # call different callback with ack requests
    
    # Callback through callback object
    cback = Callback()
    client.on_message = cback.on_message
    client.on_disconnect = on_disconnect

    connected = False    
    # Make connection attempts
    for i in range(attempts):
        try:    
            # Connect to MQTT service
            client.connect(ip, port)
            # If successful
            print(f"({ip}) Connected to broker", flush=True)
            connected = True
            break

        except ConnectionError:
            # if Connection failed
            # Sleep for 5 seconds before trying again
            print(f"({ip}) Could not connect, trying again in 5 seconds", flush=True)
            time.sleep(5)
    
    # If connection attempts fail
    if not connected:
        raise ConnectionError(f"Could not connect to {ip}")
    else:
        return client, cback


## Subscibe to hear content from data-injector
def subscribe(client, topic):
    '''
    Subscribes to topic on EQMX broker
    NOTE: will loop forever on thread to listen for messages
    Attributes:
        client: blocking connection to 
        queue: Topic to subscribe to
    '''
    # ensure topic is a string
    assert type(topic) == str

    # Subscribe to topic and wait for message
    client.subscribe(topic)
    client.loop_forever()

        
## Publisher to send heartbeat message to data injector
def publish_to_mqtt(client, topic, msg, retain=False):
    '''
    Publishes to EMQX message broker
    This sends a "heartbeat" message to show data-injector that data-preprocessor is active
    This only happens when "heartbeat" message is received from data-processor

    Attributes:
        channel: client connection to broker
        queue_name: Name of topic to publish to 
        msg: Message payload
            (note) has to be str, int, float, None or bytearray

    Returns: None

    Raises:
        AssertionError: if :msg: is not str, int, float, None or bytearray
    '''
    # Assert parameters are correct types
    assert type(topic) == str
    assert type(msg) == str or\
            type(msg) == int or\
            type(msg) == float or\
            type(msg) == None or\
            type(msg) == bytearray

    # Publish message to MQTT
    # Note: MQTT payload must be a string, bytearray, int, float or None
    msg = json.dumps(msg)
    client.publish(topic, msg, retain=retain)

def on_disconnect(client, userdata, msg):
    '''Callback for when data-preprocessor is disconnected from broker'''
    raise ConnectionAbortedError("Connection failed")


## MESSAGE CALLBACKS ##
## Call back for recieving message from data-injector
def on_message_ack(client, userdata, msg):
    ''' 
    Callback for when heartbeat message is received from data-processor
    on "check" topic
    '''
    # Parse payload
    str_msg = str(json.loads(msg.payload))

    # Check if message is a ready message
    if str_msg == "ack_req":
        print("Recieved ACK_REQUEST from publisher", flush=True)
        publish_to_mqtt(client, "response", "ack", retain=True)
        print("Sent ACK to publisher", flush=True)
    else:
        print(f"{str_msg} is not ack_req", flush=True)

## Call back object for recieving a message from data-processor
class Callback(preprocessing.Preprocessor):
    """
    Callback class to access received messages from data-processor
    When message is received, on_message is called
    Inherits Preprocessor class for stream calculations
    """
    def __init__(self):
        ''' Initialises Preprocessing and class variables'''
        # Inherit preprocesssor init
        super().__init__()
        self.rabbit_channel = None
    
    # When client recieves message
    def on_message(self, client, userdata, msg):
        '''
        Function which is called when messages is received.
        Calculates week average and publishes it to RabbitMQ message broker
        on CSC8112 queue.
        '''
        # Parse payload
        str_msg = str(json.loads(msg.payload))
        print(f"Message recieved from data-injector: {str_msg}", flush=True)

        # Process value
        avg_value = self.process_value(str_msg)
        try:
            if str_msg == "finished":
                publisher.publish_to_rabbitmq(self.rabbit_channel, "CSC8112", str_msg)
                return
        except:
            raise ValueError()

        
        if not avg_value is False and not avg_value is None:
            # Send value to RabbitMQ server
            print(f"Weekly average value: {avg_value}", flush=True)
            publisher.publish_to_rabbitmq(self.rabbit_channel, "CSC8112", avg_value)
