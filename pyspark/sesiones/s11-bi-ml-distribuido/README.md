# S11 · BI/ML distribuido con Spark

Equipo 04 · Big Data · Ciclo 9 · Abel Ángel Sullón Macalupú  
Universidad Peruana Unión · Juliaca · 2026-2

El analista de monitoreo consulta el tráfico en el rectángulo de Perú, los pronósticos de ambas dimensiones y su error cuando se conoce el valor real. [Tablero BI/ML de Grafana](http://localhost:43000/d/aerostreamperu-s11).

| Parte | Fuente | Publicación |
|---|---|---|
| A · BI batch | Parquet de observabilidad de S9, lotes con `_SUCCESS` | Un envío al Pushgateway con el total y la proporción de capturas válidas, y la latencia media de telemetría |
| B · ML en vivo | Ventanas de S8 y resultados de los `PipelineModel` de S5 redesplegados en S10 | `foreachBatch` actualiza Pushgateway a medida que Kafka entrega nuevos registros. Prometheus lee los valores cada 15 segundos |

## Ejecución

Abre Docker Desktop. Desde la raíz del proyecto, levanta la infraestructura una vez. Si Kafka, PySpark y Grafana ya están arriba, pasa a los dos procesos:

```powershell
.\iniciar.ps1 infra
```

Abre dos terminales PowerShell en la raíz. En la primera arranca el productor:

```powershell
.\iniciar.ps1 productor
```

En la segunda arranca el consumidor:

```powershell
.\iniciar.ps1 consumidor
```

Deja ambos procesos activos. El consumidor publica una vez los KPIs batch disponibles y luego arranca el único `spark-submit` continuo con S8, S9, S10 y S11. Grafana lee Prometheus, que consulta Pushgateway. Para cambiar de versión, detén esos dos procesos con Ctrl+C y vuelve a iniciarlos. Los checkpoints conservan el avance.

## Lectura del tablero

- **Seguimiento en vivo** muestra la última captura válida de OpenSky y los dos pronósticos para la siguiente ventana de diez minutos. La línea OpenSky se actualiza cada dos minutos e indica el conteo de aeronaves en vuelo para Leonardo y la mediana de sus velocidades válidas para Joel. El campo «Hora de la captura en el gráfico» permite comprobar cuándo se actualizó esa lectura.
- **Persistencia** copia el indicador de la ventana de origen de diez minutos. Por ejemplo, un promedio de 17,60 aeronaves en la ventana de origen se convierte en un pronóstico de 17,60 para la siguiente. El cuaderno conserva el valor de la última ventana cerrada para revisar ese cálculo.
- **Comparación en la misma ventana** une el inicio del objetivo de S10 con el inicio de la observación de S8. El error aparece con un objetivo de al menos cuatro capturas válidas y un pronóstico emitido antes de que termine ese objetivo. El watermark puede hacer que se emita varios minutos después de que empezó el objetivo.
- **Fecha de emisión y fecha del objetivo** permiten distinguir una actualización a tiempo de una recuperación tardía y entender cuándo se midió el último error. El eje temporal de las series representa el momento de consulta de Prometheus.
- **Pronóstico preferido** según S5. Leonardo conserva persistencia por su RMSE de validación y publica la regresión lineal base para compararla. Joel conserva Lasso. Ambos usan sus contratos guardados y sus cuadernos muestran su aporte.

Pushgateway conserva el último valor publicado por grupo. Para interpretar caídas o datos antiguos, revisa las horas de captura y emisión y el tablero S9. Usamos el mismo Pushgateway para el batch breve y la publicación continua, como en la práctica del docente. La Regla #37 motiva medir en el flujo real el error para la misma ventana pronosticada.

## Cuadernos y evidencia

- [Leonardo · volumen](11_bi_ml_distribuido_leonardo.ipynb)
- [Joel · velocidad](11_bi_ml_distribuido_joel.ipynb)

Cada cuaderno consulta la foto batch, el pronóstico de su integrante y dos instantes distintos de Prometheus si existen en el rango. Para el PDF individual de S11, conserva las capturas con reloj y perfil visibles, los dos resultados de PromQL y el enlace al repositorio.

En [Prometheus](http://localhost:49090/query), las consultas para guardar la evidencia son:

```promql
aerostream_bi_pronostico_mllib{job="aerostream_bi_leonardo"}
aerostream_bi_pronostico_mllib{job="aerostream_bi_joel"}
```

Busca el cálculo de la foto en `publicar_kpis()`, el envío continuo en `publicar_lote()` y el cruce del pronóstico con lo observado en `metricas_actuales()`, en [bi_ml.py](bi_ml.py). La unión conserva el inicio exacto del objetivo y el integrante, incluso si Kafka entrega primero la observación y después el pronóstico.
