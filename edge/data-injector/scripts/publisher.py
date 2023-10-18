from paho.mqtt import client as mqtt_client
import time
import json

def connect_to_mqtt(ip, port=1883, attempts=10):
    # Create a mqtt client object
    client = mqtt_client.Client()
    client.on_message = on_message
    client.on_disconnect = on_disconnect
    
    connected = False
    for _ in range(attempts):
        try:
            # Attempt connection to MQTT service
            client.connect(ip, port)
             
            # If successful
            print(f"Connected to {ip}", flush=True)
            connected = True
            break
        except:
            # Sleep for 5 seconds before trying again
            print(f"({ip}) could not connect, trying again in 5 seconds", flush=True)
            time.sleep(5)
        
    if not connected:
        raise ConnectionError(f"Could not connect to {ip}")
    else:
        return client

def publish_to_mqtt(client, topic, msg, retain=False):
    assert type(topic) == str
    assert type(msg) == str or\
            type(msg) == int or\
            type(msg) == float or\
            type(msg) == None or\
            type(msg) == bytearray
    
    # Publish message to MQTT
    msg = json.dumps(msg)
    return client.publish(topic, msg, retain=retain)
    
def on_message():
    pass

def on_disconnect():
    pass
