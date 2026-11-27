# Práctica de temperatura en AeroStreamPeru

La rama usa el Kafka, PySpark, Pushgateway, Prometheus y Grafana del equipo. El flujo de práctica agrega Wokwi y el puente de la S07. Úsalo para prepararte antes de rendir y aplica las reglas de tu evaluación. [Enunciado de la variante](https://262bigdata.github.io/lambda26/examenes/examen-final-practico-v4-temperatura/).

## 1. Clona y levanta los servicios

Instala Git y Docker Desktop en Windows. Abre Docker Desktop en modo de contenedores Linux. Ejecuta desde PowerShell:

```powershell
git clone --branch practica/temperatura https://github.com/LeoNarD1812/AeroStreamPeru.git
Set-Location AeroStreamPeru
.\practica-temperatura.ps1 -Modo infra -Identificador leonardoperez
```

Reemplaza **leonardoperez** por tu nombre y apellido en minúsculas, sin tildes ni espacios. Si AeroStreamPeru es privado, usa una cuenta de GitHub con acceso. La primera puesta en marcha descarga o construye imágenes Docker y puede demorar. Si la política local de PowerShell bloquea la ejecución de scripts, puedes ejecutar este repositorio revisado con una configuración limitada a esa sesión:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy RemoteSigned
```

El script usa el arranque del proyecto, construye el puente y guarda el identificador para el notebook. Crea **pyspark/.env** con un token local si el archivo no existe. Copia el valor de **JUPYTER_TOKEN** de ese archivo cuando Jupyter lo pida. Crea también la carpeta que PySpark monta para OpenSky, que puede estar vacía para esta práctica.

Puedes volver a ejecutar el modo **infra** sin repetir tu identificador. Usa un identificador por corrida y conserva el que corresponde a tus artefactos.

| Servicio | Dónde abrirlo |
| --- | --- |
| Jupyter | [Cuaderno de temperatura](http://localhost:4488/notebooks/examen-temperatura/temperatura.ipynb) |
| Kafka UI | [localhost:48085](http://localhost:48085) |
| Pushgateway | [localhost:49091](http://localhost:49091) |
| Prometheus | [localhost:49090](http://localhost:49090) |
| Grafana | [Temperatura en vivo](http://localhost:43000/d/aerostreamperu-temperatura) |

**El dashboard de Grafana aparece automáticamente en la PC nueva.** Está versionado en la carpeta de dashboards que Grafana carga al arrancar. En un entorno limpio usa **admin / admin** si tu equipo no configuró otro acceso. Las dos líneas se llenan después de que el notebook publique y Prometheus lea las métricas. El selector de la parte superior permite elegir tu job.

## 2. Arranca el puente y arma Wokwi

En otra terminal, desde la raíz del clon:

```powershell
.\practica-temperatura.ps1 -Modo puente
```

Espera **connected**. Deja abierta esa terminal. El script crea tu tópico Kafka y el puente se suscribe a tu tópico MQTT. [S07 · Puente y ESP32](https://262bigdata.github.io/lambda26/sesiones/S07_Ingesta_Eventos_IoT_Sensores_Tiempo_Real/).

1. Abre un [proyecto ESP32 genérico en Wokwi](https://wokwi.com/projects/new/esp32).
2. En **uso-atmos-examen/wokwi-local/<tu-identificador>** están los archivos preparados. Pega **diagram.json** y **sketch.ino** en las pestañas correspondientes de Wokwi. El sketch ya contiene tu identificador.
3. En **Library Manager** agrega **PubSubClient**, **DHT sensor library**, **Adafruit Unified Sensor** y **ArduinoJson 7**. Los nombres figuran también en **libraries.txt**.
4. Pulsa **Start Simulation**. Espera **MQTT conectado** y **Publicado** en el monitor serie. En la terminal del puente debe aparecer **forwarded**. En Kafka UI abre **Topics → <tu-identificador>-temperatura-eventos → Messages** y verifica que la llave es **esp32-<tu-identificador>**.
5. Haz clic en el DHT22 y mueve su deslizador de **temperatura** varias veces, durante todo el historial. Apunta a 10 minutos de lecturas. La variante acepta un mínimo de 5.

El diagrama cablea **DHT22 VCC → ESP32 3V3**, **GND → GND** y **SDA/DATA → GPIO 4**. El pin **NC** queda libre. El WiFi simulado es **Wokwi-GUEST**. Wokwi y el puente se comunican a través del broker público **test.mosquitto.org:1883**.

## 3. Entrena, ejecuta y demuestra

En **temperatura.ipynb**, ejecuta las celdas de código en orden. La primera carga el identificador guardado por el script. La segunda guarda en Parquet las lecturas válidas de los últimos 10 minutos. La tercera muestra el corte cronológico, el RMSE de persistencia, el de regresión y la ruta del modelo.

La cuarta celda inicia la consulta **foreachBatch** y publica en Pushgateway. La quinta muestra las predicciones de nuevas lecturas. La sexta imprime las dos consultas PromQL por si quieres comprobarlas manualmente en Prometheus. Para ensayar P3 por separado, primero pon **PUBLICAR_METRICAS = False** en la celda 4 y lánzala. Cambia a **True** y vuelve a ejecutarla para P4.

Mueve el sensor, abre [Grafana](http://localhost:43000/d/aerostreamperu-temperatura) y espera el siguiente scrape de Prometheus, que corre cada 15 segundos. Para la revisión, muestra el cambio en el monitor serie, Kafka UI, el notebook, Prometheus y las dos líneas del panel.

| Pregunta | Evidencia |
| --- | --- |
| P1 · Ingesta | Monitor serie y tópico Kafka con la llave del sensor. |
| P2 · Entrenamiento | Parquet y conteo, etiqueta con lead, corte temporal, dos RMSE y modelo guardado. |
| P3 · Inferencia | Filas nuevas con medición y predicción en el notebook o en Parquet. |
| P4 · Grafana | Dos métricas en Prometheus y cambio en vivo al mover el DHT22. |

Al terminar, ejecuta **consulta.stop()** en Jupyter y pulsa **Ctrl+C** en la terminal del puente. Si vas a continuar otro día, conserva los contenedores de Kafka. Si se recreó Kafka y Spark reclama offsets antiguos, detén la consulta y usa una carpeta nueva para los artefactos en la celda 1.

Con Docker activo, si un puerto está ocupado revisa qué otro stack lo usa. AeroStreamPeru y lambda26 emplean varios puertos en común. Si no hay datos, sigue la ruta Wokwi → puente → Kafka UI → notebook → Pushgateway → Prometheus → Grafana.
