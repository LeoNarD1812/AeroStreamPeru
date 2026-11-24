# AeroStreamPeru en otra laptop

## Arranque en Windows

1. Instala Git y Docker Desktop con contenedores Linux. Abre Docker Desktop antes de empezar.
2. Extrae el ZIP desde el Explorador o con 7-Zip. Abre PowerShell dentro de la carpeta `AeroStreamPeru` extraída, donde está `iniciar.ps1` y hay una carpeta `pyspark`.
3. Crea los dos archivos de configuración local:

   ```powershell
   Copy-Item .env.example .env
   Copy-Item pyspark/.env.example pyspark/.env
   notepad .env
   notepad pyspark/.env
   ```

   En `.env`, ajusta `OPENSKY_CREDENTIALS_FILE` a la ruta de tu propio JSON OAuth de OpenSky. Usa barras `/` y conserva las comillas alrededor de la ruta. En `pyspark/.env`, pon tu token local en `JUPYTER_TOKEN`. El JSON OAuth se guarda fuera de la carpeta del proyecto.
4. Verifica los datos y los modelos que vienen en el ZIP. El histórico va en `pyspark/sesiones/s01-arquitectura/data/2026/`. Los modelos de S5 y sus contratos están bajo `pyspark/sesiones/s05-evaluacion-unidad-1/artifacts/leonardo/` y `.../artifacts/joel/`. Puedes probar el flujo en vivo con esos modelos guardados. Para reproducir el entrenamiento, ejecuta los cuadernos de S5 de uno en uno.
5. Levanta los servicios y abre dos terminales PowerShell en la raíz:

   ```powershell
   .\iniciar.ps1 infra
   # Terminal 1
   .\iniciar.ps1 productor
   # Terminal 2
   .\iniciar.ps1 consumidor
   ```

   Las imágenes se preparan en el primer arranque. El productor muestra `"estado": "ok"` cuando publica una captura válida. En [Jupyter](http://localhost:4488) entra con tu token. Verifica la ingesta en [Kafka UI](http://localhost:48085), la operación en [Grafana S9](http://localhost:43000/d/aerostreamperu-s09) y tus pronósticos en [Grafana S11](http://localhost:43000/d/aerostreamperu-s11).

El primer arranque empieza con Kafka y los checkpoints de esa laptop. Grafana S11 muestra pronósticos después de que Spark cierra una ventana apta. Para cargar los KPIs batch en un entorno recién extraído, deja que la primera captura quede registrada en el Parquet de S9. Luego reinicia solo el consumidor con Ctrl+C y `.\iniciar.ps1 consumidor`. El productor puede seguir corriendo.

Para las pruebas reproducibles de S8, sigue el procedimiento de [S8](../pyspark/sesiones/s08-streaming-estructurado/README.md). Los tópicos del entorno de Leonardo se reconstruyen en la laptop de Joel al ejecutar las pruebas.

## Subir los cuadernos de Joel a GitHub

Leonardo sube primero el código común y sus propios cuadernos a `main`. Da a Joel acceso como colaborador del repositorio y pide que acepte la invitación. Joel usa su propia cuenta de GitHub.

1. Extrae el ZIP en una carpeta distinta a la del clon. Clona el repositorio y crea una rama:

   ```powershell
   git clone https://github.com/LeoNarD1812/AeroStreamPeru.git
   cd AeroStreamPeru
   git switch -c joel/cuadernos
   ```

2. Copia desde el ZIP extraído únicamente los cuadernos `*_joel.ipynb`, respetando sus carpetas de sesión. En PowerShell desde el clon:

   ```powershell
   $origenSesiones = 'C:\Ruta\Al\ZIP\Extraido\AeroStreamPeru\pyspark\sesiones'
   Get-ChildItem -LiteralPath $origenSesiones -Recurse -File -Filter '*_joel.ipynb' |
       ForEach-Object {
           $rutaRelativa = $_.FullName.Substring($origenSesiones.Length + 1)
           Copy-Item -LiteralPath $_.FullName -Destination (Join-Path 'pyspark/sesiones' $rutaRelativa)
       }
   ```

3. Comprueba la lista antes de subir:

   ```powershell
   git add -- ':(glob)pyspark/sesiones/*/*_joel.ipynb'
   git diff --cached --name-only
   git commit -m "Agrega cuadernos de Joel para las unidades I y II"
   git push -u origin joel/cuadernos
   ```

   Luego abre un *pull request* de `joel/cuadernos` a `main`. Puede ejecutar el proyecto desde el ZIP extraído y usar el clon para subir sus cuadernos. Así el clon solo gestiona el código y las contribuciones de Git.
