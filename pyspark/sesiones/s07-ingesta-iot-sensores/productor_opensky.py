"""Captura OpenSky en una cuadrícula UTC de dos minutos y publica telemetría en Kafka."""

import argparse
from datetime import datetime, timezone
import json
import math
import os
from pathlib import Path
import re
from statistics import median_low
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

CADENCIA = 120
RETRASO_PUBLICACION = 10
TOPIC_ESTADOS = "opensky.telemetria.v1"
TOPIC_CAPTURAS = "opensky.capturas.v1"
PARTICIONES = 3
RECTANGULO = {"lamin": -18.35, "lamax": -0.04, "lomin": -81.33, "lomax": -68.65}
URL_ESTADOS = "https://opensky-network.org/api/states/all"
URL_TOKEN = (
    "https://auth.opensky-network.org/auth/realms/opensky-network/"
    "protocol/openid-connect/token"
)
ICAO24 = re.compile(r"[0-9a-f]{6}")


class RespuestaInvalida(ValueError):
    """El esquema de la respuesta o su tiempo es incompatible."""


class RangoInvalido(ValueError):
    """El estado no tiene coordenadas que pertenezcan a la zona de estudio."""


def utc_iso(segundos):
    return datetime.fromtimestamp(segundos, timezone.utc).isoformat()


def leer_json(respuesta):
    try:
        return json.load(respuesta)
    except (ValueError, UnicodeError) as exc:
        raise RespuestaInvalida("La respuesta no es JSON válido") from exc


class OpenSky:
    def __init__(self, ruta_credenciales):
        credenciales = json.loads(ruta_credenciales.read_text(encoding="utf-8-sig"))
        self.cliente = credenciales.get("clientId")
        self.secreto = credenciales.get("clientSecret")
        if not self.cliente or not self.secreto:
            raise ValueError("El archivo OAuth debe tener clientId y clientSecret")
        self.token = None
        self.renovar_en = 0

    def token_actual(self):
        if self.token and time.monotonic() < self.renovar_en:
            return self.token
        form = urlencode({
            "grant_type": "client_credentials",
            "client_id": self.cliente,
            "client_secret": self.secreto,
        }).encode()
        solicitud = Request(URL_TOKEN, data=form, headers={
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
        })
        with urlopen(solicitud, timeout=25) as respuesta:
            resultado = leer_json(respuesta)
        self.token = resultado["access_token"]
        self.renovar_en = time.monotonic() + max(0, int(resultado["expires_in"]) - 60)
        return self.token

    def consultar(self, instante):
        url = f"{URL_ESTADOS}?{urlencode({**RECTANGULO, 'time': instante})}"
        for intento in range(2):
            solicitud = Request(url, headers={
                "Accept": "application/json",
                "Authorization": f"Bearer {self.token_actual()}",
            })
            try:
                with urlopen(solicitud, timeout=25) as respuesta:
                    return leer_json(respuesta), respuesta.headers.get("X-Rate-Limit-Remaining")
            except HTTPError as exc:
                if exc.code != 401 or intento:
                    raise
                self.token = None


def numero_o_nulo(valor):
    if valor is None:
        return None
    if isinstance(valor, bool) or not isinstance(valor, (int, float)) or not math.isfinite(valor):
        raise RespuestaInvalida("Se esperaba un número finito o null")
    return float(valor)


def decodificar_estado(vector, instante):
    if not isinstance(vector, list) or len(vector) < 17:
        raise RespuestaInvalida("Se esperaba un vector OpenSky de al menos 17 campos")
    codigo = vector[0].strip().lower() if isinstance(vector[0], str) else ""
    if not ICAO24.fullmatch(codigo) or not isinstance(vector[8], bool):
        raise RespuestaInvalida("icao24 u onground inválidos")
    callsign = vector[1].strip() if isinstance(vector[1], str) else ""
    return {
        "version_esquema": 1,
        "fuente": "opensky_rest",
        "event_id": f"{instante}:{codigo}",
        "capture_id": f"opensky:{instante}",
        "icao24": codigo,
        "time": instante,
        "callsign": callsign or None,
        "lastposupdate": numero_o_nulo(vector[3]),
        "lastcontact": numero_o_nulo(vector[4]),
        "lon": numero_o_nulo(vector[5]),
        "lat": numero_o_nulo(vector[6]),
        "baroaltitude": numero_o_nulo(vector[7]),
        "onground": vector[8],
        "velocity": numero_o_nulo(vector[9]),
        "heading": numero_o_nulo(vector[10]),
        "vertrate": numero_o_nulo(vector[11]),
        "geoaltitude": numero_o_nulo(vector[13]),
        "squawk": vector[14] if isinstance(vector[14], str) else None,
        "spi": vector[15] if isinstance(vector[15], bool) else None,
    }


def validar_rango_geografico(estado):
    latitud, longitud = estado["lat"], estado["lon"]
    if (
        latitud is None or longitud is None
        or not RECTANGULO["lamin"] <= latitud <= RECTANGULO["lamax"]
        or not RECTANGULO["lomin"] <= longitud <= RECTANGULO["lomax"]
    ):
        raise RangoInvalido("Coordenadas ausentes o fuera del rectángulo")


def normalizar_captura(respuesta, instante):
    if (
        not isinstance(respuesta, dict)
        or respuesta.get("time") != instante
        or "states" not in respuesta
    ):
        raise RespuestaInvalida("La API no devolvió el instante solicitado o faltó states")
    vectores = respuesta["states"]
    if vectores is None:
        vectores = []
    if not isinstance(vectores, list):
        raise RespuestaInvalida("states debe ser una lista o null")

    estados = []
    descartados_esquema = descartados_rango = 0
    for vector in vectores:
        try:
            estado = decodificar_estado(vector, instante)
            validar_rango_geografico(estado)
        except RespuestaInvalida:
            descartados_esquema += 1
        except RangoInvalido:
            descartados_rango += 1
        else:
            estados.append(estado)
    return estados, {
        "estados_recibidos": len(vectores),
        "descartados_esquema": descartados_esquema,
        "descartados_rango": descartados_rango,
    }


def asegurar_topicos(servidor):
    admin = KafkaAdminClient(bootstrap_servers=servidor, request_timeout_ms=15000)
    try:
        for nombre in (TOPIC_ESTADOS, TOPIC_CAPTURAS):
            try:
                admin.create_topics([NewTopic(nombre, PARTICIONES, 1)])
            except TopicAlreadyExistsError:
                pass
        limite = time.monotonic() + 12
        while True:
            topicos = admin.describe_topics([TOPIC_ESTADOS, TOPIC_CAPTURAS])
            if len(topicos) == 2 and all(not topico["error_code"] for topico in topicos):
                for topico in topicos:
                    if len(topico["partitions"]) != PARTICIONES:
                        raise RuntimeError(f"{topico['name']} debe tener {PARTICIONES} particiones")
                break
            if time.monotonic() >= limite:
                raise RuntimeError("Kafka no confirmó los dos tópicos de OpenSky")
            time.sleep(0.5)
    finally:
        admin.close()


def resumir_aeronaves(estados):
    """Indicadores de una sola captura. Los modelos usan ventanas de diez minutos."""
    unicos = {}
    # Ante un ICAO repetido conserva el estado con contacto y posición más recientes.
    def prioridad(estado):
        return tuple(estado[campo] if estado[campo] is not None else -math.inf
                     for campo in ("lastcontact", "lastposupdate"))

    for estado in estados:
        codigo = estado["icao24"]
        anterior = unicos.get(codigo)
        if anterior is None or prioridad(estado) > prioridad(anterior):
            unicos[codigo] = estado
    en_vuelo = [estado for estado in unicos.values() if not estado["onground"]]
    velocidades = [
        estado["velocity"] for estado in en_vuelo
        if estado["velocity"] is not None and 10 < estado["velocity"] < 350
    ]
    return {
        "aeronaves_en_vuelo": len(en_vuelo),
        "velocidad_mediana_m_s": median_low(velocidades) if velocidades else None,
    }


def publicar_captura(productor, opensky, instante):
    registro = {
        "version_esquema": 1,
        "capture_id": f"opensky:{instante}",
        "fuente": "opensky_rest",
        "time": instante,
        "cadencia_segundos": CADENCIA,
        "estados_publicados": 0,
    }
    reintento_en = 0
    try:
        respuesta, creditos = opensky.consultar(instante)
        estados, calidad = normalizar_captura(respuesta, instante)
    except HTTPError as exc:
        registro.update(estado="fallida", error=f"http_{exc.code}")
        if exc.code == 429:
            espera = exc.headers.get("X-Rate-Limit-Retry-After-Seconds", "")
            reintento_en = time.time() + (int(espera) if espera.isdecimal() else CADENCIA)
    except (URLError, TimeoutError, RespuestaInvalida, KeyError) as exc:
        registro.update(estado="fallida", error=type(exc).__name__)
    else:
        recibida_utc = utc_iso(time.time())
        enviados = []
        for estado in estados:
            estado["recibido_utc"] = recibida_utc
            enviados.append(productor.send(TOPIC_ESTADOS, key=estado["icao24"], value=estado))
        particiones = {envio.get(timeout=30).partition for envio in enviados}
        descartados = calidad["descartados_esquema"] + calidad["descartados_rango"]
        registro.update(
            calidad,
            estado="parcial" if descartados else "ok",
            estados_publicados=len(enviados),
            creditos_remanentes=int(creditos) if creditos and creditos.isdecimal() else None,
            particiones_telemetria=sorted(particiones),
        )
        if not descartados:
            registro.update(resumir_aeronaves(estados))

    registro["registrado_utc"] = utc_iso(time.time())
    productor.send(TOPIC_CAPTURAS, key=registro["capture_id"], value=registro).get(timeout=30)
    print(f"{utc_iso(instante)} {json.dumps(registro, ensure_ascii=False)}", flush=True)
    return registro["estado"] != "fallida", reintento_en


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--capturas", type=int, default=0, help="Cantidad de intentos; 0 mantiene el productor activo")
    args = parser.parse_args()
    if args.capturas < 0:
        parser.error("--capturas no puede ser negativo")

    servidor = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    credenciales = Path(os.environ.get("OPENSKY_CREDENTIALS_PATH", "/run/secrets/opensky"))
    opensky = OpenSky(credenciales)
    asegurar_topicos(servidor)
    productor = KafkaProducer(
        bootstrap_servers=servidor,
        acks="all",
        enable_idempotence=True,
        request_timeout_ms=10000,
        delivery_timeout_ms=30000,
        key_serializer=lambda clave: clave.encode("utf-8"),
        value_serializer=lambda valor: json.dumps(valor, allow_nan=False).encode("utf-8"),
    )
    intentos = exitos = 0
    siguiente = int(time.time() - RETRASO_PUBLICACION) // CADENCIA * CADENCIA
    no_consultar_antes = 0
    try:
        while not args.capturas or intentos < args.capturas:
            momento = max(siguiente + RETRASO_PUBLICACION, no_consultar_antes)
            time.sleep(max(0, momento - time.time()))
            instante = max(siguiente, int(time.time() - RETRASO_PUBLICACION) // CADENCIA * CADENCIA)
            publicada, no_consultar_antes = publicar_captura(productor, opensky, instante)
            intentos += 1
            exitos += publicada
            siguiente = instante + CADENCIA
        return 0 if exitos == intentos else 1
    finally:
        productor.close(timeout=10)


if __name__ == "__main__":
    raise SystemExit(main())

