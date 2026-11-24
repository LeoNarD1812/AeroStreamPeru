"""S11. KPIs de S9 y seguimiento de las predicciones de S10 en Grafana."""

from contextlib import closing
from datetime import timezone
from itertools import chain
import math
import os
from pathlib import Path
import sqlite3
import time
from urllib.request import Request, urlopen

from pyspark.sql import functions as F

SESION = Path(__file__).resolve().parent
OBSERVACIONES = SESION.parent / "s09-observabilidad-pipeline" / "artifacts" / "observaciones"
PUSHGATEWAY = os.environ.get("PUSHGATEWAY_URL", "http://pushgateway:9091")
INDICADORES = {"leonardo": "volumen_promedio_t", "joel": "velocidad_mediana_t"}
CAPTURAS_MINIMAS = 4
VENTANA_SEGUNDOS = 600

ESQUEMA_VENTANA = """
    inicio_utc TIMESTAMP, capturas_observadas LONG, version_esquema INT,
    volumen_promedio_t DOUBLE, velocidad_mediana_t DOUBLE
"""
ESQUEMA_PRONOSTICO = """
    inicio_objetivo_utc TIMESTAMP, emitida_utc TIMESTAMP, version_esquema INT,
    integrante STRING, prediccion_mllib DOUBLE, prediccion_persistencia DOUBLE,
    prediccion_preferida DOUBLE
"""
ESQUEMA_CAPTURA = """
    time LONG, estado STRING, version_esquema INT,
    aeronaves_en_vuelo LONG, velocidad_mediana_m_s DOUBLE
"""


def publicar(grupo, metricas):
    lineas = [
        f"aerostream_bi_{nombre} {valor}"
        for nombre, valor in metricas.items()
        if valor is not None and math.isfinite(valor)
    ]
    solicitud = Request(
        f"{PUSHGATEWAY}/metrics/job/{grupo}",
        data=("\n".join(lineas) + "\n").encode("utf-8"),
        headers={"Content-Type": "text/plain; version=0.0.4"},
        method="PUT",
    )
    with urlopen(solicitud, timeout=10):
        pass


def publicar_kpis(spark):
    lotes = sorted(str(marca.parent) for marca in OBSERVACIONES.glob("lote=*/_SUCCESS"))
    if not lotes:
        raise ValueError(f"S11 requiere lotes cerrados de S9 en {OBSERVACIONES}")
    observaciones = spark.read.parquet(*lotes)
    # Una captura por instante. Si hubo un reintento correcto, cuenta como válida.
    capturas = observaciones.filter("tipo = 'captura' AND time IS NOT NULL")
    calidad = capturas.groupBy("time").agg(F.max(F.col("isValid").cast("int")).alias("valida"))
    resumen = calidad.agg(
        F.count("*").alias("total"),
        F.coalesce(F.sum("valida"), F.lit(0)).alias("validas"),
        F.max("time").alias("ultima"),
    ).first()
    if not resumen.total:
        raise ValueError("El Parquet de S9 todavía no contiene capturas de OpenSky")
    latencia = observaciones.filter("tipo = 'estado' AND latencyMs >= 0").agg(
        (F.avg("latencyMs") / 1000).alias("segundos")
    ).first()
    metricas = {
        "batch_capturas": resumen.total,
        "batch_capturas_validas_ratio": resumen.validas / resumen.total,
        "batch_latencia_segundos": latencia.segundos,
        "batch_ultima_captura_segundos": resumen.ultima,
        "batch_calculado_segundos": time.time(),
    }
    publicar("aerostream_bi_batch", metricas)
    print(f"S11 KPIs de S9: {metricas}", flush=True)


def iniciar_publicacion(spark, prefijo, leer_kafka):
    carpeta = SESION / "artifacts" / prefijo
    carpeta.mkdir(parents=True, exist_ok=True)
    ventanas = f"{prefijo}.ventanas.v1"
    pronosticos = f"{prefijo}.predicciones.v1"
    entrada = leer_kafka(spark, [ventanas, pronosticos]).select(
        "topic",
        F.when(F.col("topic") == ventanas, F.from_json(F.col("value").cast("string"), ESQUEMA_VENTANA)).alias("ventana"),
        F.when(F.col("topic") == pronosticos, F.from_json(F.col("value").cast("string"), ESQUEMA_PRONOSTICO)).alias("pronostico"),
    )
    return (
        entrada.writeStream.queryName("bi_ml")
        .foreachBatch(lambda lote, _: publicar_lote(lote, carpeta / "comparaciones.sqlite", prefijo))
        .option("checkpointLocation", str(carpeta / "checkpoint"))
        .trigger(processingTime="5 seconds")
        .start()
    )


def iniciar_capturas(spark, prefijo, leer_kafka):
    carpeta = SESION / "artifacts" / prefijo
    carpeta.mkdir(parents=True, exist_ok=True)
    capturas = (
        leer_kafka(spark, [f"{prefijo}.capturas.v1"])
        .select(F.from_json(F.col("value").cast("string"), ESQUEMA_CAPTURA).alias("captura"))
        .select("captura.*")
        .filter("version_esquema = 1 AND estado = 'ok' AND time > 0 AND aeronaves_en_vuelo >= 0")
    )
    grupo = "aerostream_bi_captura" if prefijo == "opensky" else f"aerostream_bi_captura_{prefijo}"
    return (
        capturas.writeStream.queryName("bi_captura")
        .foreachBatch(lambda lote, _: publicar_captura(lote, grupo, carpeta / "ultima_captura.txt"))
        .option("checkpointLocation", str(carpeta / "checkpoint_capturas"))
        .trigger(processingTime="5 seconds")
        .start()
    )


def publicar_captura(lote, grupo, archivo):
    actual = lote.orderBy(F.desc("time")).first()
    if actual is None or (archivo.exists() and actual.time < int(archivo.read_text())):
        return
    publicar(grupo, {
        "captura_inicio_segundos": actual.time,
        "captura_aeronaves_en_vuelo": actual.aeronaves_en_vuelo,
        "captura_velocidad_mediana_m_s": actual.velocidad_mediana_m_s,
    })
    temporal = archivo.with_suffix(".tmp")
    temporal.write_text(str(actual.time), encoding="utf-8")
    temporal.replace(archivo)


def publicar_lote(lote, archivo, prefijo):
    filas = lote.toLocalIterator()
    primera = next(filas, None)
    if primera is None:
        return
    # Kafka puede entregar duplicados o invertir el orden entre tópicos.
    # Las claves sobreviven al reinicio y permiten emparejar ambos órdenes.
    with closing(sqlite3.connect(archivo)) as datos:
        datos.executescript("""
            CREATE TABLE IF NOT EXISTS observado (
                integrante TEXT, inicio INTEGER, valor REAL, capturas INTEGER,
                PRIMARY KEY (integrante, inicio)
            );
            CREATE TABLE IF NOT EXISTS prediccion (
                integrante TEXT, objetivo INTEGER, emitida INTEGER,
                mllib REAL, persistencia REAL, preferida REAL,
                PRIMARY KEY (integrante, objetivo)
            );
        """)
        with datos:
            for fila in chain((primera,), filas):
                guardar_fila(datos, fila)
        for integrante in INDICADORES:
            metricas = metricas_actuales(datos, integrante)
            if metricas:
                alcance = "" if prefijo == "opensky" else f"_{prefijo}"
                publicar(f"aerostream_bi_{integrante}{alcance}", metricas)
                print(f"S11 {integrante}: {metricas}", flush=True)


def guardar_fila(datos, fila):
    ventana = fila.ventana
    if ventana and ventana.version_esquema == 1 and ventana.inicio_utc and ventana.capturas_observadas is not None:
        inicio = segundos_utc(ventana.inicio_utc)
        for integrante, indicador in INDICADORES.items():
            datos.execute(
                "INSERT OR REPLACE INTO observado VALUES (?, ?, ?, ?)",
                (integrante, inicio, ventana[indicador], ventana.capturas_observadas),
            )

    p = fila.pronostico
    if p and p.version_esquema == 1 and p.integrante in INDICADORES and p.inicio_objetivo_utc:
        valores = (p.prediccion_mllib, p.prediccion_persistencia, p.prediccion_preferida)
        if all(valor is not None and math.isfinite(valor) for valor in valores):
            emitida = segundos_utc(p.emitida_utc) if p.emitida_utc else None
            datos.execute("""
                INSERT INTO prediccion VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT (integrante, objetivo) DO UPDATE SET
                    emitida = excluded.emitida, mllib = excluded.mllib,
                    persistencia = excluded.persistencia, preferida = excluded.preferida
                WHERE prediccion.emitida IS NULL OR excluded.emitida < prediccion.emitida
                """, (p.integrante, segundos_utc(p.inicio_objetivo_utc), emitida, *valores))


def segundos_utc(fecha):
    # Los timestamps del conector Spark se materializan con la zona del driver (UTC).
    return int(fecha.replace(tzinfo=timezone.utc).timestamp())


def metricas_actuales(datos, integrante):
    metricas = {}
    observado = datos.execute("""
        SELECT inicio, valor, capturas FROM observado WHERE integrante = ? ORDER BY inicio DESC LIMIT 1
        """, (integrante,)).fetchone()
    if observado:
        inicio, valor, capturas = observado
        metricas.update(observado_inicio_segundos=inicio, observado=valor, observado_capturas=capturas)

    prediccion = datos.execute("""
        SELECT objetivo, emitida, mllib, persistencia, preferida FROM prediccion
        WHERE integrante = ? ORDER BY objetivo DESC LIMIT 1
        """, (integrante,)).fetchone()
    if prediccion:
        objetivo, emitida, mllib, persistencia, preferida = prediccion
        metricas.update(
            pronostico_objetivo_segundos=objetivo, pronostico_emitido_segundos=emitida,
            pronostico_mllib=mllib, pronostico_persistencia=persistencia, pronostico_preferido=preferida,
        )
        if emitida is not None:
            metricas["pronostico_demora_inicio_objetivo_segundos"] = emitida - objetivo

    # El error solo usa observaciones con cobertura suficiente y pronósticos
    # de emisión conocida que llegaron antes del fin de su ventana objetivo.
    par = datos.execute("""
        SELECT p.objetivo, o.valor, p.mllib, p.persistencia, p.preferida
        FROM prediccion p JOIN observado o ON o.integrante = p.integrante AND o.inicio = p.objetivo
        WHERE p.integrante = ? AND o.capturas >= ? AND o.valor IS NOT NULL
            AND p.emitida < p.objetivo + ?
        ORDER BY p.objetivo DESC LIMIT 1
        """, (integrante, CAPTURAS_MINIMAS, VENTANA_SEGUNDOS)).fetchone()
    if par:
        objetivo, real, mllib, persistencia, preferida = par
        metricas.update(
            evaluado_objetivo_segundos=objetivo, evaluado_real=real,
            evaluado_mllib=mllib, evaluado_persistencia=persistencia,
            error_absoluto_mllib=abs(real - mllib), error_absoluto_persistencia=abs(real - persistencia),
            error_absoluto_preferido=abs(real - preferida),
        )
    return metricas
