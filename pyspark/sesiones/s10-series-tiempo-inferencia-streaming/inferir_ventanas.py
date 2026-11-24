"""S10. Puntúa ventanas cerradas de AeroStreamPeru con los modelos de S5."""

import argparse
import json
from pathlib import Path
import sys

from pyspark.ml import PipelineModel
from pyspark.sql import functions as F

SESION = Path(__file__).resolve().parent
ARTIFACTS_S5 = SESION.parent / "s05-evaluacion-unidad-1" / "artifacts"
sys.path.insert(0, str(SESION.parent / "s08-streaming-estructurado"))
from procesar_ventanas import KAFKA, crear_topicos, leer_kafka, sesion_spark  # noqa: E402


def cargar_modelos():
    modelos = {}
    for integrante in ("leonardo", "joel"):
        carpeta = ARTIFACTS_S5 / integrante
        archivo = carpeta / "contrato_modelo_historico.json"
        contrato = json.loads(archivo.read_text(encoding="utf-8"))
        if contrato["version"] != 1 or contrato["horizonte_minutos"] != 10:
            raise ValueError(f"Contrato no compatible: {archivo}")
        variables = [variable["nombre"] for variable in contrato["variables_predictoras"]]
        modelo = PipelineModel.load(str(carpeta / "modelo_historico"))
        if list(modelo.stages[0].getInputCols()) != variables:
            raise ValueError(f"Las predictoras difieren del modelo guardado de {integrante}")
        modelos[integrante] = contrato, modelo
    return modelos


def inferir(entradas, modelos):
    tipos = {
        "window_id": "string",
        "inicio_utc": "timestamp",
        "fin_utc": "timestamp",
        "capturas_observadas": "bigint",
        "version_esquema": "int",
    }
    for contrato, _ in modelos.values():
        for variable in contrato["variables_predictoras"]:
            nombre, tipo = variable["nombre"], variable["tipo_spark"]
            if tipos.setdefault(nombre, tipo) != tipo:
                raise ValueError(f"Tipo incompatible entre modelos: {nombre}")

    esquema = ", ".join(f"{nombre} {tipo}" for nombre, tipo in tipos.items())
    ventanas = (
        entradas.select(F.from_json(F.col("value").cast("string"), esquema).alias("ventana"))
        .select("ventana.*")
        .filter(
            (F.col("version_esquema") == 1)
            & (F.col("window_id") == F.concat(F.lit("ventana:"), F.unix_seconds("inicio_utc").cast("string")))
            & (F.col("fin_utc") == F.col("inicio_utc") + F.expr("INTERVAL 10 MINUTES"))
            & F.col("capturas_observadas").between(0, 5)
        )
    )

    resultados = []
    for integrante, (contrato, modelo) in modelos.items():
        columna_modelo = f"prediccion_{integrante}"
        ventanas = (
            modelo.transform(ventanas)
            .withColumnRenamed("prediction", columna_modelo)
            .drop("features", *modelo.stages[0].getOutputCols())
        )
        actual = F.col(contrato["indicador_actual"])
        prediccion_modelo = F.col(columna_modelo)
        apta = (
            (F.col("capturas_observadas") >= contrato["capturas_minimas"])
            & actual.isNotNull() & (~F.isnan(actual))
            & prediccion_modelo.isNotNull() & (~F.isnan(prediccion_modelo))
        )
        preferido = contrato["preferido_en_validacion"]
        resultados.append(F.struct(
            apta.alias("apta"),
            F.lit(integrante).alias("integrante"),
            F.lit(contrato["variable_objetivo"]).alias("variable_objetivo"),
            F.lit(contrato["unidad"]).alias("unidad"),
            F.lit(contrato["estimador_mllib"]).alias("modelo"),
            actual.alias("valor_actual"),
            prediccion_modelo.alias("prediccion_mllib"),
            actual.alias("prediccion_persistencia"),
            F.lit(preferido).alias("metodo_preferido"),
            (actual if preferido == "Persistencia" else prediccion_modelo).alias("prediccion_preferida"),
        ))

    return (
        ventanas.select(
            "window_id", "inicio_utc", "capturas_observadas",
            F.col("fin_utc").alias("inicio_objetivo_utc"),
            (F.col("fin_utc") + F.expr("INTERVAL 10 MINUTES")).alias("fin_objetivo_utc"),
            F.explode(F.array(*resultados)).alias("resultado"),
        )
        .select("window_id", "inicio_utc", "capturas_observadas", "inicio_objetivo_utc", "fin_objetivo_utc", "resultado.*")
        .filter("apta")
        .drop("apta")
        .withColumn("version_esquema", F.lit(1))
        .withColumn("emitida_utc", F.current_timestamp())
        .withColumn("prediction_id", F.concat_ws(":", "window_id", "integrante"))
    )


def iniciar_inferencia(spark, prefijo):
    modelos = cargar_modelos()
    destino = f"{prefijo}.predicciones.v1"
    crear_topicos([destino])
    predicciones = inferir(leer_kafka(spark, [f"{prefijo}.ventanas.v1"]), modelos)
    return (
        predicciones.select(
            F.col("prediction_id").alias("key"),
            F.to_json(F.struct(*predicciones.columns)).alias("value"),
        )
        .writeStream.queryName("inferencia")
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA)
        .option("topic", destino)
        .option("checkpointLocation", str(SESION / "artifacts" / "checkpoints" / prefijo))
        .outputMode("append")
        .trigger(processingTime="5 seconds")
        .start()
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--prefijo", default="opensky")
    parser.add_argument("--hasta-disponible", action="store_true")
    args = parser.parse_args()
    spark = sesion_spark("AeroStreamPeru-S10")
    consulta = None
    try:
        consulta = iniciar_inferencia(spark, args.prefijo)
        print(f"S10 activo: {args.prefijo}.predicciones.v1", flush=True)
        if args.hasta_disponible:
            consulta.processAllAvailable()
        else:
            consulta.awaitTermination()
    finally:
        if consulta is not None:
            consulta.stop()
        spark.stop()


if __name__ == "__main__":
    main()

