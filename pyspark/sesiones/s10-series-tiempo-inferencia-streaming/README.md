# S10 · Inferencia en streaming de AeroStreamPeru

Spark Structured Streaming lee una fila por ventana cerrada de `opensky.ventanas.v1` y aplica los `PipelineModel` de S5. Las salidas se publican en `opensky.predicciones.v1`. La consulta de inferencia funciona por fila. La ventana, el watermark y la deduplicación de estados se resuelven en S8.

## Selección de modelos y entradas

| Integrante | Objetivo de la siguiente ventana | Estimador MLlib guardado | Preferido por RMSE de validación |
|---|---|---|---|
| Leonardo | `volumen_promedio_t1` | Regresión lineal base | Persistencia |
| Joel | `velocidad_mediana_t1` | Lasso | Lasso |

S5 construye la etiqueta con `lead()`, conserva solo pares de ventanas contiguas y separa entrenamiento, validación y prueba por fecha. Calcula la persistencia antes de ajustar candidatos. S10 usa el contrato de cada integrante para validar los nombres y tipos de las predictoras. Los modelos incluyen la imputación y `VectorAssembler`. El proceso usa únicamente datos de la ventana de origen y requiere al menos cuatro de las cinco capturas programadas.

## Arranque integrado

Con Kafka, PySpark y la observabilidad de S9 levantados, ejecuta en PowerShell desde la raíz del proyecto:

```powershell
docker compose --env-file pyspark/.env -f pyspark/compose.yml -f pyspark/compose.kafka.yml exec pyspark spark-submit --driver-memory 768m --conf spark.jars.ivySettings=/opt/s08-streaming-estructurado/ivysettings.xml --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0 /opt/s08-streaming-estructurado/procesar_ventanas.py --observabilidad --inferencia
```

En otra terminal, ejecuta el productor autenticado. Ambos procesos se pueden detener con **Ctrl+C**. Para procesar únicamente lo disponible en Kafka y salir, agrega `--hasta-disponible` al comando de Spark.

```powershell
docker compose --env-file .env -f uso-opensky/compose.yml run --rm productor
```

La inferencia puede ejecutarse por separado si S8 ya produjo ventanas. Para usar una reproducción de S8, toma el prefijo del tópico en `s08-streaming-estructurado/artifacts/evidencia_s08.json` y pásalo con `--prefijo` a `inferir_ventanas.py` bajo el mismo `spark-submit`. El modo `prueba` de los notebooks mira este tópico aislado.

- [S10 Leonardo](10_inferencia_leonardo.ipynb)
- [S10 Joel](10_inferencia_joel.ipynb)

## Mensaje Kafka y tiempo de emisión

- **Identidad** · `prediction_id`, formado por la ventana de origen y el integrante. Kafka usa esta identidad como clave y el consumidor puede deduplicarla.
- **Horizonte** · `inicio_utc`, `inicio_objetivo_utc` y `fin_objetivo_utc` en UTC. `emitida_utc` registra cuándo se generó el resultado.
- **Pronósticos** · `prediccion_mllib`, `prediccion_persistencia` y `prediccion_preferida`, con el nombre del modelo y la unidad.
- **Calidad** · `capturas_observadas` identifica la cobertura de la ventana de origen.

S8 publica después de cerrar la ventana de origen. Con un watermark de cuatro minutos y una fuente que avanza a tiempo, la predicción puede salir cuando la ventana objetivo ya empezó. En una recuperación, también puede emitirse después de que terminó. El tablero debe conservar la hora de emisión y la ventana objetivo para que la audiencia pueda interpretar esos casos.

