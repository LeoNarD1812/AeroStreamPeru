# S8 · Structured Streaming de AeroStreamPeru

El proceso usa el único contenedor de PySpark. Lee `opensky.telemetria.v1` y `opensky.capturas.v1` desde Kafka. Las ventanas cerradas salen por `opensky.ventanas.v1`.

## Cómo se forman las ventanas

1. `normalizar()` usa un esquema explícito para leer el JSON mediante `from_json`. Verifica las claves, el instante, la versión y los campos obligatorios. Con `dropDuplicatesWithinWatermark` conserva un registro por identificador dentro del margen de cuatro minutos y publica la salida intermedia en `opensky.validada.v1`.
2. `ventanas_cerradas()` agrupa por diez minutos UTC usando el tiempo de evento de OpenSky. Una ventana puede cerrarse cuando el watermark supera su final. Cuatro minutos equivalen a dos intervalos de captura y permiten que un retraso moderado se incorpore antes del cierre. El cierre depende del avance del tiempo de evento y de los microlotes.
3. `resumir_lote()` verifica que cada captura exitosa tenga el número de aeronaves distintas que se confirmó desde S7. Reutiliza el cálculo de características de S5 y produce una fila por ventana con volumen, velocidad, predictoras y cobertura.

Una confirmación `ok` con cero estados cuenta como captura observada y puede aportar un volumen de cero. Una confirmación fallida, parcial o sin todos los estados esperados cuenta como faltante. Una ventana con eventos de control y cero capturas válidas se publica con cobertura cero e indicadores nulos.

Cada consulta mantiene sus offsets y su estado en un checkpoint propio bajo `artifacts/checkpoints/`. Kafka puede recibir una salida repetida si un fallo ocurre después de escribir y antes de confirmar el microlote. La salida usa una clave estable `window_id` para que el consumidor pueda deduplicarla. [Guía de Spark y Kafka](https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html) · [Deduplicación con watermark](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.dropDuplicatesWithinWatermark.html)

## Prueba controlada

Con Kafka y PySpark conectados, desde PowerShell en la raíz del repositorio:

```powershell
docker compose --env-file pyspark/.env -f pyspark/compose.yml -f pyspark/compose.kafka.yml exec -T pyspark spark-submit --driver-memory 768m --conf spark.jars.ivySettings=/opt/s08-streaming-estructurado/ivysettings.xml --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0 /opt/s08-streaming-estructurado/probar_s08.py
```

La prueba reenvía estados reales del Parquet Bronze y controla su orden de llegada. Usa tópicos aislados y confirmaciones de prueba para los casos de cero, falla e incompletitud. Verifica un duplicado, una captura que entra a tiempo pese al retraso, otra que queda fuera del watermark y la recuperación después de reiniciar ambas consultas con los mismos checkpoints. Deja un archivo de evidencia que pueden leer los dos notebooks.

- [S8 Leonardo · Volumen](08_ventanas_leonardo.ipynb)
- [S8 Joel · Velocidad mediana](08_ventanas_joel.ipynb)

## Proceso continuo

En PowerShell, inicia el consumidor de S8 y deja la terminal abierta. Detén el proceso con **Ctrl+C**.

```powershell
docker compose --env-file pyspark/.env -f pyspark/compose.yml -f pyspark/compose.kafka.yml exec pyspark spark-submit --driver-memory 768m --conf spark.jars.ivySettings=/opt/s08-streaming-estructurado/ivysettings.xml --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0 /opt/s08-streaming-estructurado/procesar_ventanas.py
```

Si ya publicaste capturas y quieres procesar solo lo que está en Kafka, agrega `--hasta-disponible` al comando de S8. Las consultas conservan su progreso y se detienen tras procesar los mensajes disponibles.

En otra terminal, inicia el productor de OpenSky siguiendo la [guía de S7](../../../uso-opensky/README.md). Para ver la salida de ese proceso en los notebooks, selecciona `MODO = "vivo"`. Grafana puede consultarse cuando esté disponible el flujo de publicación de sus métricas.

