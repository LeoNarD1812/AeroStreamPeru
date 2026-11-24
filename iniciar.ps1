param(
    [Parameter(Mandatory)]
    [ValidateSet("infra", "productor", "consumidor")]
    [string]$Modo
)

function Ejecutar-Docker {
    docker @args
    if ($LASTEXITCODE -ne 0) {
        throw "Docker terminó con código $LASTEXITCODE."
    }
}

Push-Location -LiteralPath $PSScriptRoot
try {
    $composeSpark = @(
        'compose', '--env-file', 'pyspark/.env',
        '-f', 'pyspark/compose.yml', '-f', 'pyspark/compose.kafka.yml'
    )
    switch ($Modo) {
        "infra" {
            Ejecutar-Docker compose -f kafka/compose.yml up -d --wait
            Ejecutar-Docker @composeSpark up -d --wait
            Ejecutar-Docker compose -f obs/compose.yml up -d --wait
        }
        "productor" {
            Ejecutar-Docker compose --env-file .env -f uso-opensky/compose.yml run --rm productor
        }
        "consumidor" {
            $spark = @(
                'spark-submit', '--driver-memory', '768m',
                '--conf', 'spark.jars.ivySettings=/opt/s08-streaming-estructurado/ivysettings.xml',
                '--packages', 'org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0',
                '/opt/s08-streaming-estructurado/procesar_ventanas.py'
            )
            $evidencia = 'pyspark/sesiones/s09-observabilidad-pipeline/artifacts/observaciones'
            if (Get-ChildItem -Path "$evidencia/lote=*/_SUCCESS" -ErrorAction SilentlyContinue | Select-Object -First 1) {
                Ejecutar-Docker @composeSpark exec -T pyspark @spark --publicar-kpis
            } else {
                Write-Host 'La foto batch se podrá publicar cuando S9 guarde capturas en Parquet.'
            }
            Ejecutar-Docker @composeSpark exec pyspark @spark --observabilidad --bi-ml
        }
    }
} finally {
    Pop-Location
}
