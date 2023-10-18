import get_data
import publisher

# Get Data
url = "http://uoweb3.ncl.ac.uk/api/v1.1/sensors/PER_AIRMON_MONITOR1135100/data/json/?endtime=20231009140308&starttime=20231001080000"
data = get_data.get_api_data(url)

# Connect to MQTT broker
MQTT_BROKER_ALIAS = "emqx-broker"
client = publisher.connect_to_mqtt(MQTT_BROKER_ALIAS)

# Publish data
print("Publishing data", flush=True)
for valuepair in data:
    publisher.publish_to_mqtt(client, "CSC8112", valuepair)   