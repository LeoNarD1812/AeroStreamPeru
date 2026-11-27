# AeroStreamPeru

Equipo 04 · Big Data · Ciclo 9 · Universidad Peruana Unión, Juliaca

El [brief del Proyecto Sello](docs/proyecto-sello/brief.md) describe el análisis batch y en tiempo real. El primer laboratorio comprueba el acceso en PySpark al Parquet histórico de OpenSky extraído con Trino.

La [guía para otra laptop](docs/puesta-en-marcha.md) describe el ZIP del equipo, la configuración local y cómo subir únicamente los cuadernos de un integrante.

La [presentación de las unidades I y II](docs/AeroStreamPeru_Equipo04_U1_U2.pptx) reúne los resultados y las capturas utilizadas para la sustentación.

La [guía de práctica de vibración](practica-vibracion/README.md) reúne el arranque para Windows, el circuito de Wokwi y la demostración en Grafana.

## Unidad 1

Desde PowerShell en la raíz del proyecto:

```powershell
# Si aún no existe pyspark/.env, cópialo y configura JUPYTER_TOKEN.
docker compose --env-file pyspark/.env -f pyspark/compose.yml up -d --build
```

Abre [Jupyter](http://localhost:4488) y usa el token configurado en `pyspark/.env`. Ambas unidades usan el mismo contenedor PySpark. Para los ejercicios batch, basta con levantar ese servicio. Cada integrante ejecuta y sube su notebook de cada sesión.

| Integrante | S1 · Arquitectura | S2 · Fundamentos | S3 · Calidad | S4 · Regresión | S5 · Producto U1 |
|---|---|---|---|---|---|
| Leonardo · Volumen en vuelo | [Notebook S1](pyspark/sesiones/s01-arquitectura/01_arquitectura_leonardo.ipynb) | [Notebook S2](pyspark/sesiones/s02-fundamentos/02_fundamentos_leonardo.ipynb) | [Notebook S3](pyspark/sesiones/s03-procesamiento-calidad-datos/03_calidad_leonardo.ipynb) | [Notebook S4](pyspark/sesiones/s04-ml-distribuido-regresion/04_ml_regresion_leonardo.ipynb) | [Notebook S5](pyspark/sesiones/s05-evaluacion-unidad-1/05_evaluacion_leonardo.ipynb) |
| Joel · Velocidad mediana | [Notebook S1](pyspark/sesiones/s01-arquitectura/01_arquitectura_joel.ipynb) | [Notebook S2](pyspark/sesiones/s02-fundamentos/02_fundamentos_joel.ipynb) | [Notebook S3](pyspark/sesiones/s03-procesamiento-calidad-datos/03_calidad_joel.ipynb) | [Notebook S4](pyspark/sesiones/s04-ml-distribuido-regresion/04_ml_regresion_joel.ipynb) | [Notebook S5](pyspark/sesiones/s05-evaluacion-unidad-1/05_evaluacion_joel.ipynb) |

Los notebooks individuales de S1 a S3 requieren el bloque `pyspark/sesiones/s01-arquitectura/data/2026/01/2026-01-01_2026-01-05.parquet`. Docker monta el histórico en `/opt/s01-arquitectura/data/2026` en solo lectura. La carpeta `data/` se comparte aparte del repositorio.

S3 escribe Silver y Gold en `pyspark/sesiones/s03-procesamiento-calidad-datos/artifacts/`, con una carpeta por integrante. Los archivos generados se excluyen de Git.

S4 entrena con la salida de muestra de S3 y guarda los modelos de práctica en `pyspark/sesiones/s04-ml-distribuido-regresion/artifacts/`.

### S5 · Producto de la Unidad I

El [producto U1](docs/proyecto-sello/u1-producto.md) reúne el pipeline y los resultados de ambos integrantes. Cada notebook de S5 prepara Gold con todos los Parquet montados si el histórico todavía no existe, verifica el particionamiento y entrena su modelo. Ejecuta los notebooks uno a la vez. Los modelos históricos y sus contratos se guardan en `pyspark/sesiones/s05-evaluacion-unidad-1/artifacts/`.

Para regenerar Gold histórico por separado:

```powershell
docker compose --env-file pyspark/.env -f pyspark/compose.yml exec -T pyspark spark-submit --driver-memory 768m /opt/s05-evaluacion-unidad-1/preparar_gold_historico.py
```

## Infraestructura de Unidad 2

Levanta Kafka antes de conectar el mismo PySpark y los servicios de observabilidad a su red. El puerto de Jupyter sigue siendo 4488. Guarda los notebooks antes de cambiar de modo porque Compose puede recrear el contenedor.

```powershell
.\iniciar.ps1 infra
```

Luego ejecuta `.\iniciar.ps1 productor` y `.\iniciar.ps1 consumidor` en dos terminales.

[Jupyter](http://localhost:4488) · [Kafka UI](http://localhost:48085) · [Grafana](http://localhost:43000) · [Prometheus](http://localhost:49090)

### S7 · Ingesta real de OpenSky

El [productor y los notebooks de S7](uso-opensky/README.md) documentan cómo publicar capturas autenticadas en Kafka y cómo verificar la telemetría desde Jupyter.

### S8 · Ventanas y recuperación en Spark

El [proceso y los notebooks de S8](pyspark/sesiones/s08-streaming-estructurado/README.md) describen la prueba con Kafka, watermark y checkpoint, y cómo arrancar el procesamiento continuo.

### S9 · Observabilidad y costos

La [guía de S9](pyspark/sesiones/s09-observabilidad-pipeline/README.md) reúne el arranque instrumentado, el tablero de Grafana y los notebooks individuales con alertas y costos.

### S10 · Inferencia en streaming

La [guía de S10](pyspark/sesiones/s10-series-tiempo-inferencia-streaming/README.md) documenta el arranque con `--inferencia`, los contratos de S5 y los notebooks de verificación.

### S11 · BI/ML distribuido con Spark

La [guía de S11](pyspark/sesiones/s11-bi-ml-distribuido/README.md) reúne los KPIs batch, los cuadernos individuales y el tablero Grafana de tráfico aéreo y pronósticos.

Para volver a trabajar solo en batch y liberar recursos:

```powershell
docker compose --env-file pyspark/.env -f pyspark/compose.yml up -d
docker compose -f obs/compose.yml stop
docker compose -f kafka/compose.yml stop
```
