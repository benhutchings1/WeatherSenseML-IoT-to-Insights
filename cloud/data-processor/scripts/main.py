from socket import socket
import pika
import subscriber
import time
import visualiser
import utils
from ml_engine import MLPredictor

RABBITMQ_ALIAS = "rabbitmq-broker"
 
if __name__ == '__main__':
    # Connect to broker
    print("Connecting to RabbitMQ broker", flush=True)
    rabbitmq_channel = subscriber.connect_to_rabbitmq(RABBITMQ_ALIAS, socket_timeout=150)

    # Send heartbeat message
    print("Sending Heartbeat message to data-preprocessor", flush=True)
    subscriber.publish_to_rabbitmq(rabbitmq_channel, "heartbeat", "ack")

    # Subscribe to topic CSC8112
    queue_name = "CSC8112"
    print(f"Subscribing to queue {queue_name}", flush=True)
    callback_data = subscriber.rabbitmq_subscribe_to_queue(rabbitmq_channel, queue_name)
    # Close connection once data recieved
    rabbitmq_channel.close()
    
    # Parse data to dataframe and print
    print("Parsing data for machine learning model", flush=True)
    data = utils.parse_data(callback_data.data)

    print("Plotting original data", flush=True)
    visualiser.plot_data(data, out="original.png")

    # Predict 
    model = MLPredictor(data)
    print("Training model", flush=True)
    model.train()
    print("Predicting from model", flush=True)
    forecast = model.predict()

    # Plot data
    print("Plotting forecasted results", flush=True)
    fig = model.plot_result(forecast)
    print(f"Saving images to forecast.png", flush=True)
    fig.savefig("forecast.png")

    # Waiting 3 mins before ending docker container to pull images
    t = 180
    print(f"Finished processing... waiting for {t}s before shutting down",flush=True)
    time.sleep(t)
    