# Brief técnico-analítico de AeroStreamPeru

## 1. Datos del equipo

**Equipo 04 · Big Data · Universidad Peruana Unión · Campus Juliaca · 2026-2**

- **Docente** — Abel Ángel Sullón Macalupú.
- **Sección** — Ciclo 9.
- **Repositorio** — [AeroStreamPeru](https://github.com/LeoNarD1812/AeroStreamPeru).
- **Topics académicos** — pendientes de configurar. Propuesta `campus-juliaca`, `semestre-2026-2`, `linea-cdia`, `tipo-ps`, `bigdata` y `grupo-04-aerostreamperu`. El identificador académico del topic para Ciclo 9 queda pendiente de verificar.

| Integrante | Dimensión a cargo |
|---|---|
| Leonardo Jesus Huaman Arhuata | Volumen de aeronaves observadas en vuelo |
| Joel Jonatan Quispe Mamani | Velocidad mediana de las aeronaves observadas en vuelo |

Cada integrante desarrolla y sustenta su indicador batch y su indicador streaming. El equipo comparte las fuentes, la preparación de datos y el tablero.

## 2. Dominio del proyecto

**Problema.** Las capturas de OpenSky contienen estados individuales de aeronaves. AeroStreamPeru los resume en ventanas comparables para que se pueda observar la evolución del volumen y la velocidad, anticipar la siguiente ventana y decidir cuándo revisar un cambio en el tráfico observado.

**Dominio y variable de interés.** Monitoreo aeronáutico. La variable es la dinámica del tráfico aéreo observado por OpenSky en el rectángulo de estudio que cubre Perú y zonas aledañas.

**Pregunta central.** ¿Cómo monitorear y anticipar, en ventanas de diez minutos, la cantidad promedio de aeronaves simultáneamente en vuelo y su velocidad mediana según las observaciones de OpenSky en la zona de estudio?

**Usuario y decisión.** El tablero está dirigido a un analista de monitoreo de tráfico aéreo. El analista compara la telemetría que está llegando, los indicadores de la última ventana y el pronóstico de la siguiente. Si el volumen o la velocidad cambia respecto a su patrón histórico, revisa la cobertura y la antigüedad de la última captura para decidir si investiga el cambio observado o el estado de la fuente.

**Tablero compartido de Grafana.** El tablero reúne ambas dimensiones y la lectura de la fuente en un solo lugar.

| Panel | Qué muestra | Cuándo se actualiza |
|---|---|---|
| Lecturas de OpenSky | Última captura recibida, hora de la lectura y estados recientes con identificador, posición, altitud y velocidad disponibles. | Con cada captura exitosa, programada cada dos minutos. |
| Volumen de aeronaves · Leonardo | Promedio observado de aeronaves simultáneamente en vuelo, referencia histórica y pronóstico de la siguiente ventana. | Al publicarse el resumen de cada ventana de diez minutos. |
| Velocidad mediana · Joel | Mediana observada en m/s, referencia histórica y pronóstico de la siguiente ventana. | Al publicarse el resumen de cada ventana de diez minutos. |
| Cobertura y error | Capturas recibidas frente a las cinco programadas, antigüedad de la última lectura y error de cada pronóstico. | Con cada captura para la cobertura y al publicarse el valor observado de la ventana objetivo para el error. |

El tablero identifica la ventana que corresponde a cada pronóstico y muestra si la fuente está en vivo o en modo de reproducción para pruebas.

**Arquitectura Lambda.** En U1, PySpark procesa el histórico, produce salidas Gold en Parquet y entrena los modelos de MLlib. En U2, un productor consulta OpenSky, publica telemetría en Kafka y Spark Structured Streaming calcula las características por ventana. Una consulta de inferencia aplica los modelos a esas características. Grafana presenta la telemetría, ambas dimensiones y sus pronósticos. Prometheus recopila métricas operativas.

**Fuente batch.** Tabla histórica `minio.osky.state_vectors_data4` de OpenSky Network, consultada en Trino entre el 1 de enero y el 23 de noviembre de 2026. El script `automatizar_opensky_parquet.py` ejecuta consultas secuenciales en bloques de hasta cinco días con Trino CLI, reduce el bloque a consultas diarias ante el límite de escaneo y convierte el CSV temporal a Parquet con PyArrow y ZSTD nivel 6. Bronze está organizado por año y mes. Contiene 71 archivos, 330,923,892 registros según los metadatos y 17 columnas por archivo. [Documentación de OpenSky Trino](https://openskynetwork.github.io/opensky-api/trino#state-vectors)

**Fuente streaming.** API autenticada de OpenSky `/api/states/all`, con una consulta cada dos minutos. Ambas unidades usan los límites originales de Bronze, latitud de −18.35 a −0.04 y longitud de −81.33 a −68.65. El productor emite los estados con `icao24` como clave y usa `time` de la respuesta como tiempo del evento. La identidad para deduplicar un estado es `(icao24, time)`. El histórico se puede reproducir para pruebas, con el modo de origen identificado. [Documentación de OpenSky REST](https://openskynetwork.github.io/opensky-api/rest.html#all-state-vectors)

**Modalidad.** Proyecto nuevo del Equipo 04.

## 3. Dimensiones de análisis y fuentes previstas

| Integrante | Dimensión | Indicador U1 batch | Indicador U2 streaming |
|---|---|---|---|
| Leonardo | Volumen de aeronaves en vuelo | Promedio de aeronaves simultáneas por ventana y pronóstico batch de la siguiente ventana | Promedio observado en vivo y pronóstico de la siguiente ventana |
| Joel | Velocidad en vuelo | Mediana histórica de velocidad por ventana y pronóstico batch de la siguiente ventana | Mediana observada en vivo y pronóstico de la siguiente ventana |

### Leonardo · Volumen de aeronaves

- **Decisión que apoya.** Identificar cuándo conviene revisar un aumento o una caída del volumen, considerando la cobertura de datos.
- **U1.** Contar identificadores `icao24` distintos por captura elegible y promediar los conteos de la ventana. Entrenar una regresión para el promedio de la siguiente ventana. Predictores candidatos de la ventana actual: volumen promedio, mínimo y máximo, velocidad mediana, altitud mediana, hora y día de la semana. El notebook presenta la serie histórica, la predicción y las métricas.
- **U2.** Calcular el mismo indicador a partir de Kafka y presentar en Grafana el volumen observado y el pronóstico de la próxima ventana. La referencia de persistencia usa el volumen de la ventana actual.
- **Requisitos.** El sistema debe contar cada aeronave una vez por captura, registrar la cobertura de cada ventana y presentar el valor observado y el pronosticado con sus intervalos correspondientes.

### Joel · Velocidad mediana

- **Decisión que apoya.** Identificar cuándo conviene revisar un cambio en la velocidad típica de las aeronaves observadas, con el volumen simultáneo como contexto.
- **U1.** Calcular la mediana de velocidad sobre observaciones elegibles con velocidad válida, en m/s. Entrenar una regresión para la mediana de la siguiente ventana. Predictores candidatos de la ventana actual: mediana y percentiles 25 y 75 de velocidad, medianas de altitud y velocidad vertical, volumen promedio, hora y día de la semana. El notebook presenta la serie histórica, la predicción y las métricas.
- **U2.** Calcular la mediana sobre los estados recibidos por Kafka y presentar en el mismo Grafana el valor observado y el pronóstico de la próxima ventana. La referencia de persistencia usa la mediana de la ventana actual.
- **Requisitos.** El sistema debe validar la velocidad, reportar el porcentaje de observaciones con velocidad válida y calcular el error cuando esté disponible el valor observado de la ventana pronosticada.

### Reglas compartidas

- Usar ventanas fijas de diez minutos, con tiempo de evento en UTC y hora de Lima para las variables de calendario y la visualización. Preparar en U1 una muestra comparable con la cadencia U2 de cinco capturas programadas por ventana.
- Registrar las capturas exitosas y distinguir un conteo válido de cero aeronaves de una captura faltante. Definir el umbral de cobertura tras el perfilado y aplicar la misma regla en ambas unidades.
- La población para volumen requiere `onground = false` y posición válida en el área de estudio. El perfilado definirá el criterio de frescura de posición y las exclusiones justificadas. El filtro de velocidad se aplica al indicador de velocidad y sus predictores.
- Comparar regresión lineal y bosque aleatorio con RMSE, MAE y R² y un corte cronológico. En U2 incluir la referencia de persistencia y comprobar que la etiqueta corresponda a la ventana contigua. Documentar el resultado y guardar el modelo seleccionado como `PipelineModel`.
- En streaming, la consulta de agregación con ventanas, watermark y checkpoint emite las características. La consulta de inferencia puntúa cada fila de características y registra la ventana objetivo. El tablero reúne volumen y velocidad observados, pronósticos y cobertura de captura.

**Alcance.** El proyecto cubre el histórico batch, la ingesta de telemetría aeronáutica, el análisis por ventana, la predicción, el tablero y la observabilidad. El alcance analítico es el rectángulo de estudio y los estados observados por OpenSky. Las operaciones de control aéreo y la ingesta de eventos empresariales quedan fuera del alcance propuesto.

**Alineación con S12.** Por indicación del docente al equipo, S6 es opcional. AeroStreamPeru omite la ingesta de eventos empresariales y demuestra el flujo de telemetría de S7 a S11 en la evaluación de U2.

## 4. Aprobación

- **Docente** — Abel Ángel Sullón Macalupú.
- **Fecha y conformidad** — pendientes.
