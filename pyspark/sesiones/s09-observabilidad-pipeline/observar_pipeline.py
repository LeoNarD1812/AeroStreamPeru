"""Observabilidad de S9 para las consultas de Spark de AeroStreamPeru."""

import json
import math
import os
from pathlib import Path
import time
from urllib.error import URLError
from urllib.request import Request, urlopen

from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQueryListener

BASE = Path(__file__).resolve().parent / "artifacts"
PUSHGATEWAY = os.environ.get("PUSHGATEWAY_URL", "http://pushgateway:9091")
ESQUEMA_OBSERVACION = """
    tipo STRING, time BIGINT, estado STRING, estados_publicados INT,
    isValid BOOLEAN, processedAt TIMESTAMP, latencyMs DOUBLE,
    inicio_utc TIMESTAMP, capturas_observadas BIGINT, capturas_faltantes BIGINT
"""


def enviar_metricas(grupo, valores):
    lineas = [
        f"{nombre} {valor}" for nombre, valor in valores.items()
        if valor is not None and math.isfinite(valor)
    ]
    if not lineas:
        return
    solicitud = Request(
        f"{PUSHGATEWAY}/metrics/job/{grupo}",
        data=("\n".join(lineas) + "\n").encode("utf-8"),
        headers={"Content-Type": "text/plain; version=0.0.4"},
        method="POST",
    )
    try:
        with urlopen(solicitud, timeout=3):
            pass
    except (URLError, TimeoutError) as exc:
        print(f"S9: Pushgateway no disponible para {grupo}: {type(exc).__name__}", flush=True)


def offset_por_particion(valor):
    if isinstance(valor, str):
        try:
            valor = json.loads(valor)
        except ValueError:
            return {}
    if not isinstance(valor, dict):
        return {}
    return {
        (topico, particion): offset
        for topico, particiones in valor.items()
        if isinstance(particiones, dict)
        for particion, offset in particiones.items()
        if isinstance(offset, int)
    }


def retraso_kafka(fuentes):
    retrasos = []
    for fuente in fuentes:
        disponibles = offset_por_particion(fuente.get("latestOffset"))
        leidos = offset_por_particion(fuente.get("endOffset"))
        retrasos.extend(max(0, final - leidos[clave]) for clave, final in disponibles.items() if clave in leidos)
    return sum(retrasos) if retrasos else None


class ProgresoSpark(StreamingQueryListener):
    def __init__(self):
        super().__init__()
        self.nombres = {}

    def onQueryStarted(self, event):
        self.nombres[str(event.id)] = event.name

    def onQueryProgress(self, event):
        progreso = json.loads(event.progress.json)
        nombre = progreso["name"]
        metricas = {"aerostream_spark_ultimo_progreso_segundos": time.time()}
        if progreso["numInputRows"]:
            archivo = BASE / f"progreso_{nombre}.json"
            temporal = archivo.with_suffix(".tmp")
            temporal.write_text(json.dumps(progreso, indent=2), encoding="utf-8")
            temporal.replace(archivo)
            metricas.update({
                "aerostream_spark_entrada_mensajes_por_segundo": progreso["inputRowsPerSecond"],
                "aerostream_spark_procesado_mensajes_por_segundo": progreso["processedRowsPerSecond"],
                "aerostream_spark_ultimo_lote_mensajes": progreso["numInputRows"],
                "aerostream_spark_ultimo_lote_segundos": progreso["durationMs"].get("triggerExecution", 0) / 1000,
            })
        retraso = retraso_kafka(progreso.get("sources", []))
        if retraso is not None:
            metricas["aerostream_spark_retraso_kafka_mensajes"] = retraso
        enviar_metricas(f"aerostream_spark_{nombre}", metricas)

    def onQueryIdle(self, event):
        nombre = self.nombres.get(str(event.id))
        if nombre:
            enviar_metricas(f"aerostream_spark_{nombre}", {
                "aerostream_spark_ultimo_progreso_segundos": time.time(),
            })

    def onQueryTerminated(self, event):
        self.nombres.pop(str(event.id), None)


def registrar_observacion(lote, identificador):
    lote.persist()
    try:
        if lote.isEmpty():
            return

        destino = BASE / "observaciones" / f"lote={identificador}"
        lote.write.mode("overwrite").parquet(str(destino))
        resumen = lote.filter((F.col("tipo") == "estado") & F.col("latencyMs").isNotNull()).agg(
            F.count("*").alias("eventos"),
            F.avg("latencyMs").alias("promedio"),
            F.percentile_approx("latencyMs", 0.95).alias("p95"),
            F.sum("bytes_kafka").alias("bytes"),
        ).first()
        if resumen.eventos:
            enviar_metricas("aerostream_telemetria", {
                "aerostream_telemetria_latencia_media_segundos": resumen.promedio / 1000,
                "aerostream_telemetria_latencia_p95_segundos": resumen.p95 / 1000,
                "aerostream_telemetria_ultimo_lote_eventos": resumen.eventos,
                "aerostream_telemetria_ultimo_lote_bytes_kafka": resumen.bytes,
            })

        captura = lote.filter(F.col("tipo") == "captura").orderBy(F.col("time").desc()).first()
        if captura:
            enviar_metricas("aerostream_captura", {
                "aerostream_opensky_ultima_captura_segundos": captura.time,
                "aerostream_opensky_ultima_captura_correcta": int(captura.isValid),
                "aerostream_opensky_ultima_captura_estados": captura.estados_publicados,
            })

        ventana = lote.filter(F.col("tipo") == "ventana").orderBy(F.col("inicio_utc").desc()).first()
        if ventana:
            enviar_metricas("aerostream_ventana", {
                "aerostream_ventana_capturas_observadas": ventana.capturas_observadas,
                "aerostream_ventana_capturas_faltantes": ventana.capturas_faltantes,
            })
    finally:
        lote.unpersist()


def iniciar_observacion(spark, kafka, nombres, checkpoints):
    BASE.mkdir(parents=True, exist_ok=True)
    eventos = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka)
        .option("subscribe", f"{nombres['validada']},{nombres['ventanas']}")
        .option("startingOffsets", "earliest")
        .load()
        .select(
            "topic", F.length("value").alias("bytes_kafka"),
            F.from_json(F.col("value").cast("string"), ESQUEMA_OBSERVACION).alias("dato"),
        )
        .select("topic", "bytes_kafka", "dato.*")
        .withColumn("tipo", F.when(F.col("topic") == nombres["ventanas"], "ventana").otherwise(F.col("tipo")))
        .filter(F.col("processedAt").isNotNull() | F.col("inicio_utc").isNotNull())
        .withColumn("processedAt", F.coalesce("processedAt", F.current_timestamp()))
        .drop("topic")
    )
    return (
        eventos.writeStream.queryName("observacion")
        .foreachBatch(registrar_observacion)
        .option("checkpointLocation", str(checkpoints / "observacion"))
        .trigger(processingTime="10 seconds")
        .start()
    )

