# AeroStreamPeru · Producto de Unidad I

Equipo 04 · Ciclo 9 · Universidad Peruana Unión, Juliaca  
Docente Abel Ángel Sullón Macalupú

El equipo construyó un pipeline batch distribuido que resume los estados históricos de OpenSky y pronostica el valor de la ventana de diez minutos siguiente. Leonardo sustenta el volumen de aeronaves en vuelo y Joel la velocidad mediana. [Brief del Proyecto Sello](brief.md)

## 1. Arquitectura Big Data seleccionada

La arquitectura Lambda permite recalcular indicadores y entrenar con el histórico en la rama batch. La rama de velocidad calculará los mismos indicadores a medida que lleguen nuevas capturas.

```text
Batch       OpenSky Trino → Bronze Parquet → Spark Silver → Gold → Spark MLlib
Velocidad   OpenSky REST → Kafka → Spark Structured Streaming → Grafana
```

La rama batch emplea datos de `minio.osky.state_vectors_data4` extraídos por bloques con Trino CLI y convertidos a Parquet con PyArrow. S5 usa los 71 archivos del 1 de enero al 23 de noviembre de 2026. Las ventanas se construyen a partir de las capturas disponibles en una cuadrícula UTC de dos minutos, comparable con la cadencia prevista de la API.

## 2. Transformaciones distribuidas con PySpark

Cada notebook S5 muestra una fecha de Bronze con esquema explícito. `filter` selecciona el intervalo UTC y las capturas alineadas, y `withColumn` convierte el tiempo Unix en timestamp. `explain(True)` expone el plan antes de ejecutar `show`. El ejercicio RDD cuenta estados en tierra y en vuelo para Leonardo, y estados en vuelo con y sin lectura de velocidad para Joel. El procesamiento histórico agrega todas las fechas de Gold en ventanas de diez minutos.

| Integrante | Resultado descriptivo de Gold | Notebook |
|---|---|---|
| Leonardo Jesus Huaman Arhuata | Promedio de aeronaves distintas por captura, con cobertura de la ventana | [S5 Leonardo](../../pyspark/sesiones/s05-evaluacion-unidad-1/05_evaluacion_leonardo.ipynb) |
| Joel Jonatan Quispe Mamani | Mediana de velocidad sobre el suelo en m/s, con percentiles y conteo de mediciones válidas | [S5 Joel](../../pyspark/sesiones/s05-evaluacion-unidad-1/05_evaluacion_joel.ipynb) |

## 3. Calidad y particionamiento analítico

- **Bronze** conserva los Parquet originales.
- **Silver** normaliza `icao24`, valida los campos esenciales, descarta duplicados exactos y resuelve conflictos por `(icao24, time)` mediante `row_number()`. La prioridad usa el último contacto, la última posición y un desempate reproducible. Una velocidad faltante se conserva como nulo.
- **Gold** conserva los indicadores y la cobertura por ventana. Se escribe en Parquet por mes y `fecha_utc`. La lectura de vuelta verifica **46,809 ventanas únicas** y el plan muestra `PartitionFilters` para una fecha concreta.

Una captura registrada sin aeronaves elegibles aporta cero al conteo. Una captura ausente reduce la cobertura. Para construir cada ejemplo de entrenamiento se requieren cuatro de las cinco capturas programadas tanto en la ventana de origen como en la objetivo. Los notebooks registran los nulos y duplicados observados en una fecha representativa. La misma función de limpieza se aplica al preparar Gold para todo el histórico.

## 4. Componente ML distribuido

Los notebooks declaran `VARIABLE_OBJETIVO` y `VARIABLES_PREDICTORAS` en la sección 2. La sección 5 construye la etiqueta de la siguiente ventana con `lead()` y verifica que los dos instantes estén separados por diez minutos. En la sección 6, `VectorAssembler` transforma las predictoras en el vector de entrada. La imputación con la mediana se ajusta sobre entrenamiento.

| Integrante | Variable objetivo | Predictoras de la ventana actual |
|---|---|---|
| Leonardo | `volumen_promedio_t1` | Volumen promedio, mínimo y máximo. Velocidad y altitud medianas. Hora y día de semana de Lima. |
| Joel | `velocidad_mediana_t1` | Percentiles 25, 50 y 75 de velocidad. Altitud y velocidad vertical medianas. Volumen promedio, hora y día de semana de Lima. |

Los modelos se entrenan con enero–agosto, se comparan con septiembre y se prueban con octubre–noviembre. Se comparan regresión lineal base, Ridge, Lasso, Elastic Net y bosque aleatorio con RMSE, MAE y R². La persistencia repite el indicador actual y proporciona una referencia en las mismas fechas. Se selecciona el estimador MLlib con menor RMSE en validación y se verifica al recargarlo.

| Integrante | Ganador MLlib | RMSE de validación, modelo / persistencia | RMSE de prueba, modelo / persistencia | MAE y R² del modelo en prueba |
|---|---|---|---|---|
| Leonardo | Regresión lineal base | 2.803 / 2.704 aeronaves | 2.261 / 2.303 aeronaves | 1.762 aeronaves · 0.885 |
| Joel | Lasso | 8.048 / 8.711 m/s | 11.169 / 12.034 m/s | 6.753 m/s · 0.541 |

Para volumen, la persistencia ganó en septiembre y la regresión ganó por un margen pequeño en prueba. Para velocidad, Lasso superó a persistencia en ambos periodos. Cada modelo recargado reprodujo las 7,762 predicciones de prueba y procesó un JSON con únicamente las predictoras. Los contratos junto a los modelos guardados especifican los tipos de entrada, el horizonte y las métricas.

## 5. Reproducción y evidencias

Ejecutar el notebook S5 de cada integrante de principio a fin, uno a la vez, según las instrucciones del [README](../../README.md). Si falta Gold histórico y están montados los Parquet de origen, el notebook lo crea. El informe personal debe acompañar las salidas de esquema y plan, los controles de calidad, la lectura particionada, la tabla de modelos y la verificación de recarga con capturas que muestren el reloj y el perfil del integrante.

