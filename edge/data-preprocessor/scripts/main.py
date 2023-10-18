import subscriber
import publisher

MQTT_BROKER_ALIAS = "emqx-broker"
RABBIT_IP = "192.168.0.100"

if __name__ == '__main__':
     # Connect to broker
     print("Connecting to EMQX Broker", flush=True)
     emqx_client, callback_obj  = subscriber.connect_to_mqtt(MQTT_BROKER_ALIAS)
     
     # Connecting to RabbitMQ broker
     print("Connecting to RabbitMQ", flush=True)
     rabbitmq_client= publisher.connect_to_rabbitmq(RABBIT_IP)

     # Pass through client information to callback object to enable message forwarding
     callback_obj.rabbit_channel = rabbitmq_client

     # Wait for heartbeat from cloud subscriber
     print("Waiting for heartbeat message from data-processor", flush=True)
     publisher.rabbitmq_subscribe_to_queue(rabbitmq_client, "heartbeat")

     # Send heartbeat message to producer to show I am alive
     print("Sending heartbeat message to data-injector", flush=True) 
     subscriber.publish_to_mqtt(emqx_client, "heartbeat", "ack", retain=True)

     # Subscribe to CSC8112 topic
     subscriber.subscribe(emqx_client, "CSC8112")

     # When message transmission has finished disconnect from clients
     rabbitmq_client.close()
     emqx_client.disconnect()