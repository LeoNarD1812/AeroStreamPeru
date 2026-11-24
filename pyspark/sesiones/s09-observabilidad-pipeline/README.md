# S9 · Observabilidad y costos

El mismo proceso de S8 puede medir la operación con `--observabilidad`. Kafka, Spark y OpenSky mantienen sus funciones. Prometheus obtiene métricas del exportador de Kafka y de Pushgateway. Grafana muestra la salud del flujo, la edad y el tamaño de la captura, la cobertura, la latencia, la tasa y el atraso.

## Levantar y verificar

Con Kafka y el único PySpark conectados, abre tres terminales en la raíz del repositorio.

1. Levanta Prometheus, Pushgateway y Grafana.

   ```powershell
   docker compose -f obs/compose.yml up -d --wait
   ```

2. Ejecuta S8 con instrumentación.

   ```powershell
   docker compose --env-file pyspark/.env -f pyspark/compose.yml -f pyspark/compose.kafka.yml exec pyspark spark-submit --driver-memory 768m --conf spark.jars.ivySettings=/opt/s08-streaming-estructurado/ivysettings.xml --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0 /opt/s08-streaming-estructurado/procesar_ventanas.py --observabilidad
   ```

   Para una prueba acotada puedes agregar `--duracion-segundos 235`. El consumidor usa los checkpoints existentes.

3. Ejecuta el productor de OpenSky.

   ```powershell
   docker compose --env-file .env -f uso-opensky/compose.yml run --rm productor --capturas 2
   ```

   Espera a que Spark procese las capturas y Prometheus haga al menos un scrape. Puedes detener el productor y el proceso de Spark con **Ctrl+C** para liberar memoria al correr los notebooks de análisis. Prometheus y Grafana conservan la evidencia.

- [Tablero de Grafana](http://localhost:43000/d/aerostreamperu-s09/aerostreamperu-observabilidad)
- [Targets de Prometheus](http://localhost:49090/targets)
- [S9 Leonardo · Cobertura y atraso](09_observabilidad_leonardo.ipynb)
- [S9 Joel · Latencia y mediciones](09_observabilidad_joel.ipynb)

## Qué medir

| Señal | Cálculo |
|---|---|
| Disponibilidad de Kafka Exporter | `up{job="kafka-exporter"}`. Indica si Prometheus pudo consultar al exportador. |
| Latencia de recepción a Spark | `processedAt - recibido_utc`, en milisegundos. Se agrega por minuto sobre los Parquet de evidencia. |
| Edad de la captura | Diferencia entre el reloj de Prometheus y el tiempo de evento de la última captura solicitada. |
| Tasa de entrada y procesamiento | `inputRowsPerSecond` y `processedRowsPerSecond` que reporta Spark en el último microlote con datos. |
| Atraso de Spark | Diferencia entre `latestOffset` y `endOffset` por tópico y partición en `lastProgress`. |
| Cobertura | Capturas completas y faltantes de la última ventana cerrada, de cinco programadas. |

Las métricas publicadas a Pushgateway pueden permanecer después de detener Spark. El panel de tiempo desde el último progreso permite distinguir una consulta activa de una lectura anterior. Los campos de observación y el último progreso con mensajes se guardan en `artifacts/` para reproducir los análisis sin volver a consultar OpenSky.

## Umbrales propuestos y costo

Cada notebook muestra tres umbrales. El atraso se compara con el volumen de dos capturas de tamaño máximo observado. La latencia P95 se contrasta con el doble del P95 medido y un margen de dos disparos de Spark. La cobertura de cuatro capturas es el mínimo del contrato de los modelos. Estos umbrales son una línea base para el laboratorio.

El escenario de referencia usa tres máquinas de **USD 24 por mes** cada una y 30 días de retención. Los notebooks proyectan el crecimiento a partir del tamaño real de los Parquet y de la media de estados por captura. Un volumen adicional cuesta **USD 0.10 por GiB-mes**. La proyección usa una muestra corta y sirve para justificar cuándo revisar capacidad, latencia o conectividad. [Precios de máquinas](https://www.digitalocean.com/pricing/droplets) · [Precios de almacenamiento](https://docs.digitalocean.com/products/volumes/details/pricing/)

