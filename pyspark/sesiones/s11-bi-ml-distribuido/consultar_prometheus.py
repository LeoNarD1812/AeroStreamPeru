"""Consultas de lectura para comprobar la publicación de S11."""

import json
import os
import time
from urllib.parse import urlencode
from urllib.request import urlopen

PROMETHEUS = os.environ.get("PROMETHEUS_URL", "http://prometheus:9090")


def consultar(operacion="query", **parametros):
    with urlopen(f"{PROMETHEUS}/api/v1/{operacion}?{urlencode(parametros)}", timeout=10) as respuesta:
        documento = json.load(respuesta)
    if documento["status"] != "success":
        raise RuntimeError(documento)
    return documento["data"]["result"]


def dos_valores_distintos(expresion, horas=3):
    fin = time.time()
    series = consultar("query_range", query=expresion, start=fin - horas * 3600, end=fin, step=15)
    if not series or not series[0]["values"]:
        return []
    valores = series[0]["values"]
    reciente = valores[-1]
    anterior = next((v for v in reversed(valores[:-1]) if float(v[1]) != float(reciente[1])), None)
    instantes = [anterior, reciente] if anterior else [reciente]
    resultados = [consultar(query=expresion, time=instante) for instante, _ in instantes]
    return [resultado[0]["value"] for resultado in resultados if resultado]
