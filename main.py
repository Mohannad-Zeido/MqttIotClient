import paho.mqtt.client as mqtt
from influxdb import InfluxDBClient
import time
import secrets

measurement_location = 0  # the position in the message corresponding to the location the measurement was taken
temperature = 1  # the position in the message corresponding to the device location
humidity = 2  # the position in the message corresponding to the device location
perceived_temperature = 3  # the position in the message corresponding to the device location

influx_db_client = None
mqtt_client = None


def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))

    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    client.subscribe("sensors/temperature")


def on_message(client, userdata, msg):
    global influx_db_client
    print("message received")
    data = msg.payload.decode("utf-8")
    print(data)
    results = data.split(",")
    measurement_json = [
            {
                "measurement": "data",
                "tags": {
                    "location": results[measurement_location],
                },
                "fields": {
                    "temperature": results[humidity],
                    "humidity": results[temperature],
                    "perceived temperature": results[perceived_temperature]
                }
            }]
    influx_db_client.write_points(measurement_json)


def setup_influxdb():
    global influx_db_client

    influx_db_client = InfluxDBClient(host=secrets.influxdb_ip_address, port=secrets.influxdb_port)
    influx_db_client.create_database(secrets.influxdb_database_name)
    influx_db_client.switch_database(secrets.influxdb_database_name)


def setup_mqtt_client():
    global mqtt_client

    mqtt_client = mqtt.Client()
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message
    mqtt_client.connect(secrets.mqtt_ip_address, secrets.mqtt_port)


def run():
    setup_influxdb()
    setup_mqtt_client()
    mqtt_client.loop_forever()


run()
