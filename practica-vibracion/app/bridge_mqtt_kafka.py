# Adaptado del puente MQTT de lambda26, sesión S07.
import json
import os
import uuid

import paho.mqtt.client as mqtt
from kafka import KafkaProducer
from kafka.errors import KafkaError


# test.mosquitto.org es un broker PÚBLICO — cualquiera en internet puede
# publicar o suscribirse a cualquier topic. "identificador propio" evita que tus mensajes
# se mezclen con los de otro equipo del curso; cámbialo por tu propio ID.
MQTT_HOST = os.getenv("MQTT_HOST", "test.mosquitto.org")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_USERNAME = os.getenv("MQTT_USERNAME", "")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD", "")
MQTT_TOPIC = os.environ["MQTT_TOPIC"]

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.environ["KAFKA_TOPIC"]

# Se comprueba la llave antes de reenviar el JSON original.
# El notebook valida el esquema y el rango de la medición.
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    key_serializer=lambda key: key.encode("utf-8"),
    value_serializer=lambda value: value,
)


def on_connect(client, userdata, flags, reason_code, properties=None):
    print(json.dumps({
        "service": "practica-vibracion",
        "component": "bridge",
        "mqttHost": MQTT_HOST,
        "mqttTopic": MQTT_TOPIC,
        "status": "connected" if reason_code == 0 else f"connect_failed:{reason_code}",
    }))
    client.subscribe(MQTT_TOPIC)


def on_message(client, userdata, msg):
    raw = msg.payload
    try:
        event = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        print("Se omitió un mensaje MQTT con JSON inválido.")
        return
    if not isinstance(event, dict):
        return
    sensor_id = event.get("sensorId")
    if not isinstance(sensor_id, str) or not sensor_id:
        return

    try:
        metadata = producer.send(KAFKA_TOPIC, key=sensor_id, value=raw).get(timeout=10)
    except KafkaError as error:
        print(f"No se pudo reenviar a Kafka: {error}")
        return

    log = {
        "service": "practica-vibracion",
        "component": "bridge",
        "mqttTopic": msg.topic,
        "kafkaTopic": metadata.topic,
        "partition": metadata.partition,
        "offset": metadata.offset,
        "sensorId": sensor_id,
        "status": "forwarded",
    }
    print(json.dumps(log))


client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"practica-vibracion-bridge-{uuid.uuid4().hex[:8]}")
if MQTT_USERNAME and MQTT_PASSWORD:
    client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
client.on_connect = on_connect
client.on_message = on_message

print(json.dumps({
    "service": "practica-vibracion",
    "component": "bridge",
    "mqttHost": MQTT_HOST,
    "mqttPort": MQTT_PORT,
    "kafkaBootstrapServers": KAFKA_BOOTSTRAP_SERVERS,
    "status": "starting",
}))

client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
client.loop_forever()
