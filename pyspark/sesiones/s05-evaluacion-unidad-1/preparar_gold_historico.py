"""Construye las ventanas del histórico que consumen los dos modelos de AeroStreamPeru."""

import json
from pathlib import Path

from pyspark.sql import SparkSession, Window, functions as F
from pyspark.sql.types import BooleanType, DoubleType, LongType, StringType, StructType

BRONZE = Path("/opt/s01-arquitectura/data/2026")
SALIDA = Path("/opt/s05-evaluacion-unidad-1/artifacts/historico")
CADENCIA_SEGUNDOS = 120
VENTANA_MINUTOS = 10
RECTANGULO = {"lat_min": -18.35, "lat_max": -0.04, "lon_min": -81.33, "lon_max": -68.65}
RANGO_VELOCIDAD_M_S = {"min_exclusivo": 10, "max_exclusivo": 350}
ESQUEMA = (
    StructType()
    .add("time", LongType())
    .add("icao24", StringType())
    .add("lat", DoubleType())
    .add("lon", DoubleType())
    .add("onground", BooleanType())
    .add("velocity", DoubleType())
    .add("baroaltitude", DoubleType())
    .add("vertrate", DoubleType())
    .add("lastposupdate", DoubleType())
    .add("lastcontact", DoubleType())
)


def limpiar_estados(muestra):
    """Devuelve los estados válidos, sin duplicados exactos y únicos por captura."""
    opcionales = ["velocity", "baroaltitude", "vertrate", "lastposupdate", "lastcontact"]
    candidatos = (
        muestra.withColumn("icao24", F.lower(F.trim("icao24")))
        .withColumns({columna: F.nanvl(F.col(columna), F.lit(None).cast("double")) for columna in opcionales})
        .na.drop(subset=["time", "icao24", "lat", "lon", "onground"])
        .filter(
            F.col("icao24").rlike(r"^[0-9a-f]{6}$")
            & F.col("lat").between(RECTANGULO["lat_min"], RECTANGULO["lat_max"])
            & F.col("lon").between(RECTANGULO["lon_min"], RECTANGULO["lon_max"])
        )
    )
    sin_exactos = candidatos.dropDuplicates()
    prioridad = Window.partitionBy("icao24", "time").orderBy(
        F.col("lastcontact").desc_nulls_last(),
        F.col("lastposupdate").desc_nulls_last(),
        F.to_json(F.struct(*sin_exactos.columns)).asc(),
    )
    unicos = (
        sin_exactos.withColumn("prioridad", F.row_number().over(prioridad))
        .filter(F.col("prioridad") == 1)
        .drop("prioridad")
    )
    return candidatos, sin_exactos, unicos


def preparar_ventanas(muestra, capturas=None):
    """Aplica la limpieza de Silver y calcula las dos dimensiones en Gold."""
    if capturas is None:
        capturas = muestra.select("time").distinct()
    _, _, unicos = limpiar_estados(muestra)
    en_vuelo = (
        unicos.filter(~F.col("onground"))
        .withColumn("captura_utc", F.timestamp_seconds("time"))
        .withColumn(
            "velocidad_valida_m_s",
            F.when(
                (F.col("velocity") > RANGO_VELOCIDAD_M_S["min_exclusivo"])
                & (F.col("velocity") < RANGO_VELOCIDAD_M_S["max_exclusivo"]),
                F.col("velocity"),
            ),
        )
    )

    conteo = en_vuelo.groupBy("time").agg(F.count("*").alias("aeronaves"))
    volumen = (
        capturas.join(conteo, "time", "left")
        .na.fill(0, subset=["aeronaves"])
        .withColumn("captura_utc", F.timestamp_seconds("time"))
        .groupBy(F.window("captura_utc", f"{VENTANA_MINUTOS} minutes").alias("ventana"))
        .agg(
            F.count("*").alias("capturas_observadas"),
            F.avg("aeronaves").alias("volumen_promedio_t"),
            F.min("aeronaves").alias("volumen_min_t"),
            F.max("aeronaves").alias("volumen_max_t"),
        )
    )
    mediciones = (
        en_vuelo.groupBy(F.window("captura_utc", f"{VENTANA_MINUTOS} minutes").alias("ventana"))
        .agg(
            F.count("*").alias("observaciones_en_vuelo"),
            F.count("velocidad_valida_m_s").alias("velocidades_validas"),
            F.percentile_approx("velocidad_valida_m_s", [0.25, 0.50, 0.75]).alias("percentiles"),
            F.percentile_approx("baroaltitude", 0.5).alias("altitud_mediana_t"),
            F.percentile_approx("vertrate", 0.5).alias("velocidad_vertical_mediana_t"),
        )
        .withColumn("velocidad_p25_t", F.get("percentiles", 0))
        .withColumn("velocidad_mediana_t", F.get("percentiles", 1))
        .withColumn("velocidad_p75_t", F.get("percentiles", 2))
        .drop("percentiles")
    )
    return (
        volumen.join(mediciones, "ventana", "left")
        .na.fill(0, subset=["observaciones_en_vuelo", "velocidades_validas"])
        .withColumn("inicio_utc", F.col("ventana.start"))
        .withColumn("fecha_utc", F.to_date("inicio_utc"))
        .withColumn("hora_lima", F.from_utc_timestamp("inicio_utc", "America/Lima"))
        .withColumn("hora_del_dia", F.hour("hora_lima"))
        .withColumn("dia_semana", F.dayofweek("hora_lima"))
        .withColumn("capturas_esperadas", F.lit(VENTANA_MINUTOS * 60 // CADENCIA_SEGUNDOS))
        .withColumn("capturas_faltantes", F.col("capturas_esperadas") - F.col("capturas_observadas"))
        .drop("ventana", "hora_lima")
    )


def preparar_historico(spark):
    meses = sorted(carpeta for carpeta in BRONZE.iterdir() if carpeta.is_dir())
    archivos = {mes.name: sorted(mes.glob("*.parquet")) for mes in meses}
    archivos = {mes: rutas for mes, rutas in archivos.items() if rutas}
    total_archivos = sum(map(len, archivos.values()))
    if not total_archivos:
        raise FileNotFoundError(f"No hay archivos Parquet en {BRONZE}")

    SALIDA.mkdir(parents=True, exist_ok=True)
    gold = SALIDA / "gold_ventanas"
    manifiesto = SALIDA / "fuente.json"
    manifiesto.unlink(missing_ok=True)
    meses_esperados = {f"mes=2026-{mes}" for mes in archivos}
    meses_previos = {ruta.name for ruta in gold.glob("mes=*") if ruta.is_dir()}
    if meses_previos - meses_esperados:
        raise ValueError(f"Meses ajenos a las fuentes actuales en Gold: {meses_previos - meses_esperados}")
    print(f"Histórico Bronze: {total_archivos} archivos en {len(archivos)} meses", flush=True)
    for mes, rutas in archivos.items():
        destino_mes = gold / f"mes=2026-{mes}"
        rutas_spark = [str(ruta) for ruta in rutas]
        tipos = {campo.name: campo.dataType for campo in spark.read.parquet(*rutas_spark).schema}
        incompatibles = [campo.name for campo in ESQUEMA if tipos.get(campo.name) != campo.dataType]
        if incompatibles:
            raise ValueError(f"Esquema incompatible en {mes}: {incompatibles}")

        muestra = (
            spark.read.schema(ESQUEMA).parquet(*rutas_spark)
            .where(F.pmod(F.col("time"), F.lit(CADENCIA_SEGUNDOS)) == 0)
            .persist()
        )
        try:
            (
                preparar_ventanas(muestra)
                .repartition(8, "fecha_utc")
                .write.mode("overwrite")
                .partitionBy("fecha_utc")
                .parquet(str(destino_mes))
            )
        finally:
            muestra.unpersist()
        obtenido = spark.read.parquet(str(destino_mes)).count()
        if not obtenido:
            raise ValueError(f"No se generaron ventanas para el mes {mes}")
        print(f"2026-{mes}: {len(rutas)} Parquet, {obtenido:,} ventanas", flush=True)

    ventanas = spark.read.parquet(str(gold))
    resumen = ventanas.agg(
        F.count("*").alias("total"),
        F.countDistinct("inicio_utc").alias("unicas"),
        F.min("inicio_utc").alias("inicio"),
        F.max("inicio_utc").alias("fin"),
        F.sum("capturas_observadas").alias("capturas"),
    ).first()
    if resumen.total <= 0 or resumen.total != resumen.unicas:
        raise ValueError(f"Ventanas Gold no únicas: {resumen}")
    plan_particion = ventanas.filter(F.col("fecha_utc") == F.lit("2026-01-01").cast("date"))
    plan_particion.explain("formatted")

    metadatos = {
        "archivos_bronze": total_archivos,
        "archivos": [ruta.relative_to(BRONZE).as_posix() for rutas in archivos.values() for ruta in rutas],
        "ventanas": resumen.total,
        "capturas_observadas": resumen.capturas,
        "primera_ventana_utc": resumen.inicio.isoformat(),
        "ultima_ventana_utc": resumen.fin.isoformat(),
        "cadencia_segundos": CADENCIA_SEGUNDOS,
        "ventana_minutos": VENTANA_MINUTOS,
        "rectangulo": RECTANGULO,
        "rango_velocidad_m_s": RANGO_VELOCIDAD_M_S,
    }
    manifiesto.write_text(json.dumps(metadatos, indent=2) + "\n", encoding="utf-8")
    print(f"Gold verificado: {resumen.total:,} ventanas, {resumen.capturas:,} capturas", flush=True)


def main():
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("AeroStreamPeru-Gold-Historico")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    try:
        preparar_historico(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
