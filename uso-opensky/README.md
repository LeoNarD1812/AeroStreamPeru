# S7 · Telemetría real de OpenSky

El productor consulta estados de aeronaves de OpenSky REST y los publica en Kafka. OpenSky entrega sus mediciones por HTTPS con OAuth2. El proceso Python actúa como puente entre la API y los tópicos del proyecto. [Documentación de OpenSky REST](https://openskynetwork.github.io/opensky-api/rest.html#all-state-vectors)

El [archivo del productor](../pyspark/sesiones/s07-ingesta-iot-sensores/productor_opensky.py) está junto a los notebooks. En Jupyter se abre desde `s07-ingesta-iot-sensores/productor_opensky.py`. Docker ejecuta ese mismo archivo mediante un montaje de solo lectura.

Para explicar el origen del dato, `URL_ESTADOS` define el endpoint `/api/states/all` y `RECTANGULO` delimita la consulta. `OpenSky.consultar()` envía la solicitud autenticada con el instante de captura. `publicar_captura()` valida la respuesta y entrega los estados a Kafka.

## Ejecutar manualmente

Desde la raíz del repositorio, con Docker Desktop activo:

```powershell
docker compose -f kafka/compose.yml up -d --wait
docker compose --env-file pyspark/.env -f pyspark/compose.yml -f pyspark/compose.kafka.yml up -d --build
docker compose --env-file .env -f uso-opensky/compose.yml run --rm --build productor --capturas 2
```

En cada equipo, configura una vez `OPENSKY_CREDENTIALS_FILE` en el `.env` de la raíz con la ruta al archivo OAuth local. Ese archivo contiene `clientId` y `clientSecret`. Docker lee la ruta desde `.env` y monta el archivo como secreto de solo lectura. Guarda el JSON fuera del repositorio. Al omitir `--capturas`, el productor consulta continuamente hasta que se detiene. Las consultas se programan en instantes UTC múltiplos de 120 segundos y se solicitan al menos 10 segundos después de ese instante. El rectángulo usa los límites del histórico Bronze. OpenSky devuelve el tiempo de la captura, que se contrasta con lo solicitado. [Autenticación y cuotas](https://openskynetwork.github.io/opensky-api/rest.html#authentication)

| Evidencia individual | Notebook |
|---|---|
| Leonardo · Conteo de aeronaves en vuelo y rango de posición | [S7 Leonardo](../pyspark/sesiones/s07-ingesta-iot-sensores/07_telemetria_leonardo.ipynb) |
| Joel · Calidad de las mediciones de velocidad | [S7 Joel](../pyspark/sesiones/s07-ingesta-iot-sensores/07_telemetria_joel.ipynb) |

En [Kafka UI](http://localhost:48085) se pueden revisar las claves, particiones y offsets.

## Contrato v1

| Tópico | Clave Kafka | Mensaje |
|---|---|---|
| `opensky.telemetria.v1` | `icao24` | Un estado por aeronave y captura. `event_id` combina `time` e `icao24`. |
| `opensky.capturas.v1` | `capture_id` | Estado de la solicitud, número de vectores recibidos y publicados, descartes y particiones usadas. |

Ambos tópicos tienen tres particiones. La confirmación de captura se publica cuando Kafka confirma sus estados. El `capture_id` enlaza los dos tópicos. Los consumidores pueden deduplicar estados por `event_id` y usar ese identificador aunque un proceso se reinicie.

| Campo de telemetría | Tipo y unidad |
|---|---|
| `version_esquema`, `fuente` | Entero `1` y cadena `opensky_rest`. |
| `capture_id`, `event_id`, `icao24` | Cadenas para enlazar, deduplicar y particionar. |
| `time`, `recibido_utc` | Tiempo del estado en segundos Unix y hora UTC de recepción en formato ISO 8601. |
| `lat`, `lon`, `onground` | Coordenadas en grados dentro del área de estudio y bandera booleana de tierra. |
| `velocity`, `vertrate` | Velocidad sobre el suelo y velocidad vertical en m/s, con valores nulos admitidos. |
| `baroaltitude`, `geoaltitude`, `heading` | Altitudes en metros y rumbo en grados, con valores nulos admitidos. |
| `lastposupdate`, `lastcontact` | Segundos Unix del origen, con valores nulos admitidos. |
| `callsign`, `squawk`, `spi` | Cadenas y bandera opcionales de OpenSky. |

El productor separa errores de esquema de coordenadas ausentes o fuera del rectángulo. El notebook de Joel aplica a velocidad el rango analítico de S5, mayor que 10 y menor que 350 m/s. Esto permite contar las aeronaves cuyo estado es válido aunque no tengan una velocidad utilizable.

- Una captura `ok` con cero estados es una respuesta exitosa.
- Una captura `parcial` conserva los estados válidos y registra cuántos descartó.
- Una captura `fallida` indica un error de la fuente o de la respuesta. El cero de estados publicados en ese caso representa falta de cobertura. Ante HTTP 429, el productor respeta el tiempo de espera que indique OpenSky.

