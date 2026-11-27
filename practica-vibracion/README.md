# Práctica de vibración en AeroStreamPeru

La práctica usa el Kafka, PySpark, Pushgateway, Prometheus y Grafana de AeroStreamPeru. Agrega un ESP32 con acelerómetro MPU6050, el puente MQTT de S07 y un notebook de predicción. La ficha compartida es la de ruido. Aquí se aplican sus mismos criterios de demostración a vibración. Usa el material para practicar antes de rendir y sigue las reglas de tu evaluación. [Variante oficial de referencia](https://262bigdata.github.io/lambda26/examenes/examen-final-practico-v4-temperatura/).

## 1. Clona y arranca en Windows

Instala Git y Docker Desktop. Abre Docker Desktop en modo de contenedores Linux y ejecuta en PowerShell:

```powershell
git clone --single-branch --branch practica/vibracion https://github.com/LeoNarD1812/AeroStreamPeru.git
cd AeroStreamPeru
.\practica-vibracion\iniciar.ps1 -Modo infra
```

El identificador inicial es **joeljhonatalquispemanani**. Si debes usar otra grafía, pasa **-Identificador** seguido del nombre completo en letras minúsculas, sin espacios ni tildes. El script genera el sketch con ese identificador y el notebook lo lee de la configuración local.

El primer arranque descarga imágenes y construye el puente. El script crea la carpeta vacía que PySpark monta para OpenSky y genera **pyspark/.env** con un token de Jupyter cuando falta. Copia el valor de **JUPYTER_TOKEN** al iniciar sesión. Si PowerShell bloquea scripts en esa terminal, puedes habilitarlos solo para la sesión con **Set-ExecutionPolicy -Scope Process -ExecutionPolicy RemoteSigned**.

| Servicio | Enlace local |
| --- | --- |
| Kafka UI | [localhost:48085](http://localhost:48085) |
| Notebook | [vibracion.ipynb](http://localhost:4488/notebooks/practica-vibracion/vibracion.ipynb) |
| Pushgateway | [localhost:49091](http://localhost:49091) |
| Prometheus | [localhost:49090](http://localhost:49090) |
| Grafana | [Vibración en vivo](http://localhost:43000/d/aerostreamperu-vibracion) |

Grafana carga el dashboard automáticamente desde el repositorio. Para una instalación limpia, las credenciales predeterminadas del equipo son **admin / admin**. El panel recibe las dos series cuando el notebook publica sus métricas. Las prácticas del curso y del equipo comparten puertos. Levanta una sola infraestructura Docker a la vez.

## 2. Inicia el puente y configura Wokwi

En otra terminal, desde la raíz del clon:

```powershell
.\practica-vibracion\iniciar.ps1 -Modo puente
```

El puente crea **joeljhonatalquispemanani-vibracion-eventos** en Kafka y escucha **lambda26/joeljhonatalquispemanani/vibracion/lecturas** en el broker público **test.mosquitto.org:1883**. Espera la línea **connected** y deja abierta la terminal. [S07 · Ingesta IoT](https://262bigdata.github.io/lambda26/sesiones/S07_Ingesta_Eventos_IoT_Sensores_Tiempo_Real/).

1. Abre un [proyecto ESP32 en Wokwi](https://wokwi.com/projects/new/esp32).
2. Desde **practica-vibracion/wokwi-local/joeljhonatalquispemanani/** copia **diagram.json** y **sketch.ino** a las pestañas correspondientes de Wokwi. Si elegiste otro identificador, abre la carpeta que imprimió el script.
3. Agrega en **Library Manager** las librerías de **libraries.txt**: PubSubClient, Adafruit MPU6050, Adafruit BusIO, Adafruit Unified Sensor y ArduinoJson 7. Si aparece la pestaña **libraries.txt**, puedes pegar la lista preparada para fijar ArduinoJson 7.4.2.
4. Pulsa **Start Simulation**. El monitor serie debe mostrar **MQTT conectado** y publicaciones. En la terminal del puente aparece **forwarded**. En Kafka UI abre el tópico y verifica que la llave sea **esp32-joeljhonatalquispemanani**.
5. Haz clic en el MPU6050 y ajusta la aceleración **X** o **Y**. Por ejemplo, mantén X en 0.2 g y luego pásala a 0.8 g. Deja cada valor unos 20 segundos para verlo en Prometheus y Grafana. Si tu vista de Wokwi no muestra el control, cambia **accelX** en el diagrama, reinicia solo Wokwi y verifica que la transmisión cambia mientras el resto sigue corriendo.

| Pin MPU6050 | Pin ESP32 DevKitC |
| --- | --- |
| VCC | 3V3 |
| GND | GND |
| SDA | GPIO 21 |
| SCL | GPIO 22 |

El MPU6050 usa I²C con dirección **0x68**. El firmware muestrea X e Y y publica una vez por segundo el RMS horizontal **sqrt(promedio(ax² + ay²))**, en **m/s²**. Z queda fuera del cálculo porque el montaje horizontal de referencia tiene 1 g en ese eje. El valor representa amplitud simulada en Wokwi. Un montaje inclinado también puede producir componente horizontal de gravedad. El notebook acepta de **0 a 28 m/s²**, adecuado para el ajuste de ±2 g por eje. [Referencia del MPU6050 en Wokwi](https://docs.wokwi.com/parts/wokwi-mpu6050).

Cada mensaje contiene **tipoEvento**, **sensorId**, **vibracion**, **origen** y **timestamp**. El timestamp usa milisegundos desde el epoch.

## 3. Entrena, ejecuta y muestra los resultados

Deja Wokwi publicando unos **10 minutos** y varía X o Y durante la captura. La referencia acepta un mínimo de 5 minutos. Abre [vibracion.ipynb](http://localhost:4488/notebooks/practica-vibracion/vibracion.ipynb) y ejecuta sus celdas en orden:

1. **Configuración**. Verifica que imprime el tópico, sensor y job de Joel.
2. **Historial**. Guarda y relee el Parquet. Muestra el conteo, los tiempos y el número de valores distintos. Si la captura es demasiado corta, espera y vuelve a ejecutar.
3. **Entrenamiento**. Construye la etiqueta de la siguiente lectura con **lead**. Muestra el corte ordenado 80/20 y compara los RMSE de persistencia y regresión lineal en el mismo test. Guarda el modelo.
4. **Inferencia**. Recarga el modelo, arranca **foreachBatch** y por defecto publica las dos métricas. Para demostrar P3 primero, usa **PUBLICAR_METRICAS = False**. Cámbialo a **True** y reejecuta para P4.
5. **Evidencia**. Muestra predicciones guardadas para lecturas nuevas en el notebook.
6. **Consultas**. Copia por separado a Prometheus las dos expresiones con el job **lambda26_joeljhonatalquispemanani_ml**.

En [Grafana](http://localhost:43000/d/aerostreamperu-vibracion) deben aparecer **Vibración real** y **Predicción siguiente**. Cambia X o Y y mantén el nuevo valor. Prometheus consulta Pushgateway cada 15 segundos. Comprueba las métricas **lambda26_ml_vibracion_real** y **lambda26_ml_vibracion_prediccion_siguiente**. El gráfico muestra la predicción en el instante de su lectura de origen.

| Bloque | Puntos de la ficha con Wokwi | Qué mostrar |
| --- | --- | --- |
| P1 | 2 + 3 | Circuito, ESP32 publicando y tópico Kafka con sensorId como llave. |
| P2 | 2 + 2 + 2 | Parquet y conteo, etiqueta y corte temporal, baseline y modelo con ambos RMSE. |
| P3 | 4 | Una predicción por lectura nueva en el notebook. |
| P4 | 2 + 1 + 1 + 1 | Pushgateway, dos métricas en Prometheus, panel y cambio en vivo desde Wokwi. |

## Si un paso falla

- **Wokwi publica, Kafka está vacío.** Comprueba el identificador del sketch y el de PowerShell. Busca **connected** y **forwarded** en el puente. Revisa que el broker público sea accesible desde la red de la PC.
- **Spark no encuentra Kafka.** Reinicia el kernel, ejecuta primero la configuración y verifica acceso a Maven Central. Usa el modo **infra** de esta carpeta para que el notebook se monte en el contenedor.
- **Grafana está vacío.** Inicia la celda 4 con publicación activa, genera lecturas nuevas y espera un scrape. Revisa el job en Pushgateway y las consultas en Prometheus.
- **Kafka se recreó y Spark reclama offsets antiguos.** Detén la consulta y usa un nombre nuevo para la carpeta de artefactos en la celda 1, así se crea un checkpoint nuevo.

Al terminar, ejecuta **consulta.stop()** en Jupyter y detén el puente con **Ctrl+C**. El Parquet, el modelo y el checkpoint quedan en **practica-vibracion/artifacts/**.

[Guía S03](https://262bigdata.github.io/lambda26/sesiones/S03_Procesamiento_Calidad_Datos_Particionamiento/) · [Guía S10](https://262bigdata.github.io/lambda26/sesiones/S10_Series_Tiempo_Inferencia_Streaming_MLlib/) · [Guía S11](https://262bigdata.github.io/lambda26/sesiones/S11_BI_ML_Distribuido_Spark_Grafana/).
