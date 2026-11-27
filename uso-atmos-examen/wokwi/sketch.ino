#include <WiFi.h>
#include <PubSubClient.h>
#include <DHT.h>
#include <ArduinoJson.h>
#include <sys/time.h>
#include <time.h>

#define IDENTIFICADOR "tunombre"
const char *SENSOR_ID = "esp32-" IDENTIFICADOR;
const char *TOPIC = "lambda26/" IDENTIFICADOR "/temperatura/lecturas";
const char *BROKER = "test.mosquitto.org";
const unsigned long INTERVALO_MS = 1000;

DHT dht(4, DHT22);
WiFiClient wifi;
PubSubClient mqtt(wifi);
unsigned long ultimaPublicacion = 0;
unsigned long ultimoIntento = 0;

void setup() {
  Serial.begin(115200);
  dht.begin();
  WiFi.begin("Wokwi-GUEST", "", 6);
  Serial.print("Conectando al WiFi");
  while (WiFi.status() != WL_CONNECTED) {
    delay(500);
    Serial.print(".");
  }
  Serial.println("\nWiFi conectado");
  configTime(0, 0, "pool.ntp.org", "time.nist.gov");
  Serial.println("Sincronizando NTP");
  while (time(nullptr) < 1700000000) {
    delay(250);
  }
  Serial.println("Reloj sincronizado");
  mqtt.setServer(BROKER, 1883);
  ultimoIntento = millis() - 2000;
}

void loop() {
  const unsigned long ahora = millis();
  if (!mqtt.connected()) {
    if (ahora - ultimoIntento >= 2000) {
      ultimoIntento = ahora;
      if (mqtt.connect(SENSOR_ID)) {
        Serial.println("MQTT conectado");
      } else {
        Serial.printf("Error MQTT: %d\n", mqtt.state());
      }
    }
    return;
  }

  mqtt.loop();
  if (ahora - ultimaPublicacion < INTERVALO_MS) return;
  ultimaPublicacion = ahora;
  const float temperatura = dht.readTemperature();
  if (isnan(temperatura) || temperatura < -40 || temperatura > 80) {
    Serial.println("Lectura fuera de rango o inválida");
    return;
  }

  timeval reloj;
  gettimeofday(&reloj, nullptr);
  const uint64_t timestamp = uint64_t(reloj.tv_sec) * 1000
                           + reloj.tv_usec / 1000;

  JsonDocument evento;
  evento["tipoEvento"] = "sensor.lectura";
  evento["sensorId"] = SENSOR_ID;
  evento["temperatura"] = temperatura;
  evento["origen"] = "wokwi";
  evento["timestamp"] = timestamp;

  char payload[256];
  serializeJson(evento, payload, sizeof(payload));
  if (mqtt.publish(TOPIC, payload)) {
    Serial.print("Publicado: ");
    Serial.println(payload);
  } else {
    Serial.println("MQTT no acepto la publicacion");
  }
}
