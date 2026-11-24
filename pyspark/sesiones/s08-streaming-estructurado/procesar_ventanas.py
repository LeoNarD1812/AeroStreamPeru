"""S8. Normaliza Kafka, deduplica y publica las ventanas cerradas de AeroStreamPeru."""

import argparse
import os
from pathlib import Path
import sys

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import IntegerType, StringType, StructType

SESION = Path(__file__).resolve().parent
sys.path.insert(0, str(SESION.parent / "s05-evaluacion-unidad-1"))
from preparar_gold_historico import (  # noqa: E402
    CADENCIA_SEGUNDOS, ESQUEMA, RECTANGULO, VENTANA_MINUTOS, preparar_ventanas,
)

KAFKA = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
WATERMARK = "4 minutes"
VENTANA = f"{VENTANA_MINUTOS} minutes"
CAPTURAS_ESPERADAS = VENTANA_MINUTOS * 60 // CADENCIA_SEGUNDOS
ESQUEMA_EVENTO = (
    StructType(list(ESQUEMA))
    .add("version_esquema", IntegerType())
    .add("capture_id", StringType())
    .add("event_id", StringType())
    .add("tipo", StringType())
    .add("id_registro", StringType())
    .add("estado", StringType())
    .add("estados_publicados", IntegerType())
)
CAMPOS_EVENTO = ESQUEMA_EVENTO.fieldNames()
ESQUEMA_NORMALIZACION = StructType(list(ESQUEMA_EVENTO)).add("recibido_utc", StringType())


def topicos(prefijo):
    return {nombre: f"{prefijo}.{nombre}.v1" for nombre in (
        "telemetria", "capturas", "validada", "ventanas"
    )}


def crear_topicos(nombres):
    admin = KafkaAdminClient(bootstrap_servers=KAFKA)
    try:
        existentes = set(admin.list_topics())
        for nombre in set(nombres) - existentes:
            try:
                admin.create_topics([NewTopic(nombre, num_partitions=3, replication_factor=1)])
            except TopicAlreadyExistsError:
                pass
    finally:
        admin.close()


def leer_kafka(spark, nombres):
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA)
        .option("subscribe", ",".join(nombres))
        .option("startingOffsets", "earliest")
        .load()
    )


def normalizar(spark, nombres):
    esquema = leer_kafka(spark, [nombres["telemetria"], nombres["capturas"]]).select(
        "topic",
        F.col("key").cast("string").alias("clave_kafka"),
        F.from_json(F.col("value").cast("string"), ESQUEMA_NORMALIZACION).alias("datos"),
    ).select("topic", "clave_kafka", "datos.*")

    captura_id = F.concat(F.lit("opensky:"), F.col("time").cast("string"))
    event_id = F.concat_ws(":", F.col("time"), F.col("icao24"))
    estado_correcto = (
        (F.col("topic") == nombres["telemetria"])
        & F.col("icao24").rlike(r"^[0-9a-f]{6}$")
        & (F.col("clave_kafka") == F.col("icao24"))
        & (F.col("event_id") == event_id)
        & F.col("lat").between(RECTANGULO["lat_min"], RECTANGULO["lat_max"])
        & F.col("lon").between(RECTANGULO["lon_min"], RECTANGULO["lon_max"])
        & F.col("onground").isNotNull()
    )
    captura_correcta = (
        (F.col("topic") == nombres["capturas"])
        & (F.col("clave_kafka") == captura_id)
        & F.col("estado").isin("ok", "parcial", "fallida")
        & (F.col("estados_publicados") >= 0)
    )
    return (
        esquema.filter(
            (F.col("version_esquema") == 1)
            & (F.col("capture_id") == captura_id)
            & (F.pmod(F.col("time"), F.lit(CADENCIA_SEGUNDOS)) == 0)
            & (estado_correcto | captura_correcta)
        )
        .withColumn(
            "tipo", F.when(F.col("topic") == nombres["telemetria"], "estado").otherwise("captura")
        )
        .withColumn(
            "id_registro",
            F.when(F.col("tipo") == "estado", F.col("event_id"))
            .otherwise(F.concat_ws(":", "capture_id", "estado", "estados_publicados")),
        )
        .withColumn("tiempo_evento", F.timestamp_seconds("time"))
        .withWatermark("tiempo_evento", WATERMARK)
        .dropDuplicatesWithinWatermark(["id_registro"])
        .withColumn("processedAt", F.current_timestamp())
        .withColumn("isValid", (F.col("tipo") == "estado") | (F.col("estado") == "ok"))
        .withColumn(
            "latencyMs",
            F.unix_millis("processedAt") - F.unix_millis(F.try_to_timestamp("recibido_utc")),
        )
        .select(
            F.col("id_registro").alias("key"),
            F.to_json(F.struct(*CAMPOS_EVENTO, "processedAt", "isValid", "latencyMs")).alias("value"),
        )
    )


def ventanas_cerradas(spark, nombres):
    eventos = (
        leer_kafka(spark, [nombres["validada"]])
        .select(F.from_json(F.col("value").cast("string"), ESQUEMA_EVENTO).alias("datos"))
        .select("datos.*")
        .withColumn("tiempo_evento", F.timestamp_seconds("time"))
        .withWatermark("tiempo_evento", WATERMARK)
    )
    return eventos.groupBy(F.window("tiempo_evento", VENTANA).alias("ventana")).agg(
        F.collect_list(F.struct(*CAMPOS_EVENTO)).alias("eventos")
    )


def resumir_lote(cerradas):
    franjas = cerradas.select(
        F.col("ventana.start").alias("inicio_utc"),
        F.col("ventana.end").alias("fin_utc"),
    )
    eventos = cerradas.select(F.explode("eventos").alias("registro")).select("registro.*")
    estados = eventos.filter(F.col("tipo") == "estado")
    esperados = (
        eventos.filter((F.col("tipo") == "captura") & (F.col("estado") == "ok"))
        .groupBy("time")
        .agg(F.max("estados_publicados").alias("esperados"))
    )
    recibidos = estados.groupBy("time").agg(F.countDistinct("icao24").alias("recibidos"))
    capturas_validas = (
        esperados.join(recibidos, "time", "left")
        .na.fill(0, subset=["recibidos"])
        .filter(F.col("esperados") == F.col("recibidos"))
        .select("time")
    )
    muestra = estados.join(capturas_validas, "time", "left_semi").select(*ESQUEMA.fieldNames())
    metricas = preparar_ventanas(muestra, capturas=capturas_validas)
    return (
        franjas.join(metricas, "inicio_utc", "left")
        .na.fill(0, subset=["capturas_observadas"])
        .withColumn("fecha_utc", F.to_date("inicio_utc"))
        .withColumn("capturas_esperadas", F.lit(CAPTURAS_ESPERADAS))
        .withColumn("capturas_faltantes", F.col("capturas_esperadas") - F.col("capturas_observadas"))
        .withColumn("version_esquema", F.lit(1))
        .withColumn("window_id", F.concat(F.lit("ventana:"), F.unix_seconds("inicio_utc").cast("string")))
    )


def publicar_lote(cerradas, _batch_id, destino):
    cerradas.persist()
    try:
        if cerradas.isEmpty():
            return
        resumen = resumir_lote(cerradas)
        (
            resumen.select(
                F.col("window_id").alias("key"),
                F.to_json(F.struct(*resumen.columns), {"ignoreNullFields": "false"}).alias("value"),
            )
            .write.format("kafka")
            .option("kafka.bootstrap.servers", KAFKA)
            .option("topic", destino)
            .save()
        )
    finally:
        cerradas.unpersist()


def iniciar(spark, nombres, checkpoints):
    validada = (
        normalizar(spark, nombres).writeStream.queryName("deduplicacion").format("kafka")
        .option("kafka.bootstrap.servers", KAFKA)
        .option("topic", nombres["validada"])
        .option("checkpointLocation", str(checkpoints / "deduplicacion"))
        .outputMode("append")
        .trigger(processingTime="5 seconds")
        .start()
    )
    try:
        agregada = (
            ventanas_cerradas(spark, nombres).writeStream.queryName("ventanas")
            .foreachBatch(lambda lote, identificador: publicar_lote(lote, identificador, nombres["ventanas"]))
            .option("checkpointLocation", str(checkpoints / "ventanas"))
            .outputMode("append")
            .trigger(processingTime="5 seconds")
            .start()
        )
    except Exception:
        validada.stop()
        raise
    return validada, agregada


def sesion_spark(nombre):
    spark = (
        SparkSession.builder.master("local[2]").appName(nombre)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--prefijo", default="opensky")
    parser.add_argument("--checkpoints", type=Path, default=SESION / "artifacts" / "checkpoints")
    parser.add_argument(
        "--hasta-disponible", action="store_true",
        help="Procesa el backlog de los tópicos y se detiene",
    )
    parser.add_argument("--observabilidad", action="store_true", help="Activa métricas y evidencia de S9")
    parser.add_argument("--duracion-segundos", type=int, help="Detención programada de una prueba en vivo")
    parser.add_argument("--inferencia", action="store_true", help="Publica los pronósticos de S10")
    parser.add_argument("--bi-ml", action="store_true", help="Activa inferencia y publicación en Grafana de S11")
    parser.add_argument("--publicar-kpis", action="store_true", help="Publica una foto batch desde el Parquet de S9 y termina")
    args = parser.parse_args()
    if args.duracion_segundos is not None and args.duracion_segundos <= 0:
        parser.error("--duracion-segundos debe ser positivo")
    nombres = topicos(args.prefijo)
    spark = sesion_spark("AeroStreamPeru-S08")
    consultas = ()
    try:
        if args.bi_ml or args.publicar_kpis:
            sys.path.insert(0, str(SESION.parent / "s11-bi-ml-distribuido"))
            from bi_ml import iniciar_capturas, iniciar_publicacion, publicar_kpis
        if args.publicar_kpis:
            publicar_kpis(spark)
            return
        crear_topicos(nombres.values())
        checkpoints = args.checkpoints / args.prefijo
        if args.observabilidad:
            sys.path.insert(0, str(SESION.parent / "s09-observabilidad-pipeline"))
            from observar_pipeline import BASE, ProgresoSpark, iniciar_observacion
            BASE.mkdir(parents=True, exist_ok=True)
            spark.streams.addListener(ProgresoSpark())
        consultas = iniciar(spark, nombres, checkpoints)
        if args.observabilidad:
            consultas += (iniciar_observacion(spark, KAFKA, nombres, checkpoints),)
        if args.inferencia or args.bi_ml:
            sys.path.insert(0, str(SESION.parent / "s10-series-tiempo-inferencia-streaming"))
            from inferir_ventanas import iniciar_inferencia
            consultas += (iniciar_inferencia(spark, args.prefijo),)
        if args.bi_ml:
            consultas += (iniciar_publicacion(spark, args.prefijo, leer_kafka),)
            consultas += (iniciar_capturas(spark, args.prefijo, leer_kafka),)
        print(f"S8 activo: {nombres['ventanas']}", flush=True)
        if args.hasta_disponible:
            for consulta in consultas:
                consulta.processAllAvailable()
        elif args.duracion_segundos is not None:
            spark.streams.awaitAnyTermination(args.duracion_segundos)
        else:
            spark.streams.awaitAnyTermination()
    finally:
        for consulta in reversed(consultas):
            consulta.stop()
        spark.stop()


if __name__ == "__main__":
    main()

