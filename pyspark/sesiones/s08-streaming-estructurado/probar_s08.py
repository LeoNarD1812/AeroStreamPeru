"""Prueba S8 con capturas reales del Parquet y casos de cobertura y tardanza controlados."""

from datetime import datetime, timezone
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import time

from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from pyspark.sql import functions as F

from procesar_ventanas import (
    KAFKA, SESION, ESQUEMA, crear_topicos, iniciar, sesion_spark, topicos,
)
from preparar_gold_historico import BRONZE

ANCLA = int(datetime(2026, 1, 4, tzinfo=timezone.utc).timestamp())
MINUTOS = [2, 4, 6, 8, 20, 22, 24, 26, 28, 36]


def leer_registros(topico, maximo_por_particion=None):
    """Lee hasta los offsets finales que Kafka tenía al comenzar la inspección."""
    consumidor = KafkaConsumer(bootstrap_servers=KAFKA, enable_auto_commit=False)
    try:
        partes = consumidor.partitions_for_topic(topico)
        if partes is None:
            raise RuntimeError(f"No se encontró {topico}")
        particiones = [TopicPartition(topico, parte) for parte in sorted(partes)]
        consumidor.assign(particiones)
        finales = consumidor.end_offsets(particiones)
        for particion, inicio in consumidor.beginning_offsets(particiones).items():
            if maximo_por_particion is not None:
                inicio = max(inicio, finales[particion] - maximo_por_particion)
            consumidor.seek(particion, inicio)
        registros = []
        limite = time.monotonic() + 20
        while any(consumidor.position(p) < finales[p] for p in particiones):
            if time.monotonic() >= limite:
                raise TimeoutError(f"No se pudo leer {topico}")
            for particion, lote in consumidor.poll(timeout_ms=500).items():
                registros.extend(
                    json.loads(mensaje.value) for mensaje in lote if mensaje.offset < finales[particion]
                )
        return registros
    finally:
        consumidor.close()


def enviar(productor, topico, clave, mensaje):
    productor.send(
        topico, key=clave.encode("utf-8"), value=json.dumps(mensaje, allow_nan=False).encode("utf-8")
    ).get(timeout=20)


def confirmacion(productor, nombres, instante, estado, cantidad):
    clave = f"opensky:{instante}"
    enviar(productor, nombres["capturas"], clave, {
        "version_esquema": 1,
        "capture_id": clave,
        "time": instante,
        "estado": estado,
        "estados_publicados": cantidad,
    })


def reproducir(productor, nombres, instante, filas, duplicar=False):
    enviados = {}
    for estado in filas:
        codigo = estado["icao24"].strip().lower()
        registro = {
            **estado,
            "icao24": codigo,
            "version_esquema": 1,
            "capture_id": f"opensky:{instante}",
            "event_id": f"{instante}:{codigo}",
        }
        enviados[codigo] = registro
        enviar(productor, nombres["telemetria"], codigo, registro)
    if duplicar:
        repetido = next(iter(enviados.values()))
        enviar(productor, nombres["telemetria"], repetido["icao24"], repetido)
    confirmacion(productor, nombres, instante, "ok", len(enviados))
    return next(iter(enviados.values()))["event_id"]


def avanzar(consultas):
    for consulta in consultas:
        consulta.processAllAvailable()


def contar_metrica(consulta, campo):
    return sum(
        operador.get(campo, operador.get("customMetrics", {}).get(campo, 0))
        for avance in consulta.recentProgress
        for operador in avance["stateOperators"]
    )


def revisar_contrato(spark, ventana):
    contratos = SESION.parent / "s05-evaluacion-unidad-1" / "artifacts"
    for persona in ("leonardo", "joel"):
        archivo = contratos / persona / "contrato_modelo_historico.json"
        contrato = json.loads(archivo.read_text(encoding="utf-8"))
        columnas = ", ".join(
            f"{campo['nombre']} {campo['tipo_spark']}" for campo in contrato["variables_predictoras"]
        )
        fila = spark.range(1).select(
            F.from_json(F.lit(json.dumps(ventana)), columnas).alias("datos")
        ).select("datos.*").first()
        assert all(fila[campo['nombre']] == ventana[campo['nombre']] for campo in contrato["variables_predictoras"])


def main():
    carpeta = SESION / "artifacts"
    carpeta.mkdir(parents=True, exist_ok=True)
    prefijo = datetime.now(timezone.utc).strftime("opensky.s08-prueba-%Y%m%d%H%M%S")
    nombres = topicos(prefijo)
    crear_topicos(nombres.values())
    spark = sesion_spark("AeroStreamPeru-S08-Prueba")
    productor = KafkaProducer(bootstrap_servers=KAFKA, acks="all", enable_idempotence=True)
    consultas = ()
    try:
        instantes = {minuto: ANCLA + minuto * 60 for minuto in MINUTOS}
        fuente = BRONZE / "01" / "2026-01-01_2026-01-05.parquet"
        filas = spark.read.schema(ESQUEMA).parquet(str(fuente)).filter(
            F.col("time").isin(list(instantes.values()))
        ).collect()
        datos = {
            minuto: [fila.asDict() for fila in filas if fila.time == instante]
            for minuto, instante in instantes.items()
        }
        assert all(datos.values()), "El bloque debe tener estados en los instantes de replay"
        print(f"Replay real en tópicos aislados: {prefijo}", flush=True)

        with TemporaryDirectory(prefix="s08-checkpoints-") as temporal:
            checkpoints = Path(temporal)
            consultas = iniciar(spark, nombres, checkpoints)
            duplicado_id = reproducir(productor, nombres, instantes[2], datos[2], duplicar=True)
            reproducir(productor, nombres, instantes[6], datos[6])
            avanzar(consultas)
            metadatos = leer_registros(nombres["validada"])
            assert sum(fila.get("event_id") == duplicado_id for fila in metadatos) == 1
            duplicados = contar_metrica(consultas[0], "numDroppedDuplicateRows")
            assert duplicados >= 1
            print("Duplicado real descartado.", flush=True)

            reproducir(productor, nombres, instantes[4], datos[4])
            avanzar(consultas)
            assert any(fila["time"] == instantes[4] for fila in leer_registros(nombres["validada"]))
            print("Captura real con dos minutos de retraso admitida.", flush=True)

            confirmacion(productor, nombres, ANCLA + 600, "ok", 0)
            confirmacion(productor, nombres, ANCLA + 720, "fallida", 0)
            confirmacion(productor, nombres, ANCLA + 840, "ok", 1)
            for minuto in (20, 22, 24, 26, 28):
                reproducir(productor, nombres, instantes[minuto], datos[minuto])
            avanzar(consultas)
            cerradas = {fila["window_id"]: fila for fila in leer_registros(nombres["ventanas"])}
            assert len(cerradas) == 2, cerradas
            volumen = cerradas[f"ventana:{ANCLA}"]
            cero = cerradas[f"ventana:{ANCLA + 600}"]
            assert volumen["capturas_observadas"] == 3 and volumen["capturas_faltantes"] == 2, volumen
            assert cero["capturas_observadas"] == 1 and cero["volumen_promedio_t"] == 0, cero
            assert cero["capturas_faltantes"] == 4 and cero["velocidad_mediana_t"] is None, cero
            print("Verificado: una captura con cero cuenta como observada; fallida e incompleta quedan como faltantes.", flush=True)

            tardio_id = reproducir(productor, nombres, instantes[8], datos[8])
            avanzar(consultas)
            assert not any(fila.get("event_id") == tardio_id for fila in leer_registros(nombres["validada"]))
            tarde = contar_metrica(consultas[0], "numRowsDroppedByWatermark")
            assert tarde > 0
            assert len(leer_registros(nombres["ventanas"])) == 2
            print(f"Datos fuera del watermark descartados: {tarde}", flush=True)

            for consulta in reversed(consultas):
                consulta.stop()
            consultas = iniciar(spark, nombres, checkpoints)
            avanzar(consultas)
            assert len(leer_registros(nombres["ventanas"])) == 2
            reproducir(productor, nombres, instantes[36], datos[36])
            avanzar(consultas)
            registros = leer_registros(nombres["ventanas"])
            assert len(registros) == 3
            recuperada = next(fila for fila in registros if fila["window_id"] == f"ventana:{ANCLA + 1200}")
            assert recuperada["capturas_observadas"] == 5
            revisar_contrato(spark, recuperada)
            print("Checkpoint recuperado sin repetir las ventanas previas. Contratos S5 compatibles.", flush=True)

            evidencia = {
                "origen": fuente.name,
                "topico_ventanas": nombres["ventanas"],
                "ventanas_finalizadas": len(registros),
                "ventana_completa": recuperada["window_id"],
                "duplicados_eliminados": duplicados,
                "registros_fuera_del_watermark": tarde,
                "checkpoint_recuperado": True,
            }
            (carpeta / "evidencia_s08.json").write_text(
                json.dumps(evidencia, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
            )
            print(json.dumps(evidencia, ensure_ascii=False), flush=True)
            for consulta in reversed(consultas):
                consulta.stop()
            consultas = ()
    finally:
        for consulta in reversed(consultas):
            consulta.stop()
        productor.close(timeout=10)
        spark.stop()


if __name__ == "__main__":
    main()

