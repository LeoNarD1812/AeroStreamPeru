#include <WiFi.h>
#include <Wire.h>
#include <PubSubClient.h>
#include <ArduinoJson.h>
#include <Adafruit_MPU6050.h>
#include <Adafruit_Sensor.h>
#include <math.h>
#include <sys/time.h>
#include <time.h>

#define IDENTIFICADOR "tunombre"
const char *SENSOR_ID = "esp32-" IDENTIFICADOR;
const char *TOPIC = "lambda26/" IDENTIFICADOR "/vibracion/lecturas";
const char *BROKER = "test.mosquitto.org";
const unsigned long INTERVALO_MUESTRA_MS = 20;
const unsigned long INTERVALO_PUBLICACION_MS = 1000;

Adafruit_MPU6050 imu;
WiFiClient wifi;
PubSubClient mqtt(wifi);
unsigned long ultimaMuestra = 0;
unsigned long ultimaPublicacion = 0;
unsigned long ultimoIntento = 0;
float sumaCuadrados = 0.0f;
unsigned int muestras = 0;

void setup() {
  Serial.begin(115200);
  Wire.begin(21, 22);
  while (!imu.begin(0x68, &Wire)) {
    Serial.println("MPU6050 no detectado: revisa SDA, SCL y alimentación.");
    delay(1000);
  }
  imu.setAccelerometerRange(MPU6050_RANGE_2_G);
  imu.setFilterBandwidth(MPU6050_BAND_21_HZ);

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
  ultimaPublicacion = millis();
}

void loop() {
  delay(5);
  const unsigned long ahora = millis();
  if (!mqtt.connected() && ahora - ultimoIntento >= 2000) {
    ultimoIntento = ahora;
    if (mqtt.connect(SENSOR_ID)) {
      Serial.println("MQTT conectado");
    } else {
      Serial.printf("Error MQTT: %d\n", mqtt.state());
    }
  }
  mqtt.loop();

  if (ahora - ultimaMuestra >= INTERVALO_MUESTRA_MS) {
    ultimaMuestra = ahora;
    sensors_event_t aceleracion, giro, temperatura;
    if (imu.getEvent(&aceleracion, &giro, &temperatura)) {
      const float x = aceleracion.acceleration.x;
      const float y = aceleracion.acceleration.y;
      if (isfinite(x) && isfinite(y)) {
        sumaCuadrados += x * x + y * y;
        muestras++;
      }
    }
  }

  if (ahora - ultimaPublicacion < INTERVALO_PUBLICACION_MS) return;
  ultimaPublicacion = ahora;
  const float rms = muestras ? sqrtf(sumaCuadrados / muestras) : NAN;
  sumaCuadrados = 0.0f;
  muestras = 0;
  if (!mqtt.connected() || !isfinite(rms)) return;

  timeval reloj;
  gettimeofday(&reloj, nullptr);
  const uint64_t timestamp = uint64_t(reloj.tv_sec) * 1000 + reloj.tv_usec / 1000;

  JsonDocument evento;
  evento["tipoEvento"] = "sensor.lectura";
  evento["sensorId"] = SENSOR_ID;
  evento["vibracion"] = rms;
  evento["origen"] = "wokwi";
  evento["timestamp"] = timestamp;

  char payload[256];
  serializeJson(evento, payload, sizeof(payload));
  if (mqtt.publish(TOPIC, payload)) {
    Serial.print("Publicado: ");
    Serial.println(payload);
  } else {
    Serial.println("MQTT no aceptó la publicación");
  }
}
