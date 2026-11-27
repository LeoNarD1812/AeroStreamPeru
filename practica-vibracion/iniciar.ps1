param(
    [Parameter(Mandatory)]
    [ValidateSet('infra', 'puente')]
    [string]$Modo,

    [ValidateScript({ $_ -cmatch '^[a-z]{2,60}$' -and $_ -cne 'tunombre' })]
    [string]$Identificador
)

$ErrorActionPreference = 'Stop'

function Ejecutar-Docker {
    docker @args
    if ($LASTEXITCODE -ne 0) {
        throw "Docker terminó con código $LASTEXITCODE."
    }
}

Push-Location -LiteralPath (Join-Path $PSScriptRoot '..')
try {
    $config = Join-Path $PSScriptRoot 'identificador.local'
    if ($Modo -eq 'puente' -and $Identificador) {
        throw 'Configura el identificador primero con -Modo infra -Identificador.'
    }

    if ($Modo -eq 'infra') {
        if ($Identificador) {
            Set-Content -LiteralPath $config -Value $Identificador -Encoding ascii
        } elseif (-not (Test-Path -LiteralPath $config)) {
            Set-Content -LiteralPath $config -Value 'joeljhonatalquispemanani' -Encoding ascii
        }
    }
    if (-not (Test-Path -LiteralPath $config -PathType Leaf)) {
        throw 'Ejecuta primero -Modo infra.'
    }
    $id = (Get-Content -LiteralPath $config -Raw).Trim()
    if ($id -cnotmatch '^[a-z]{2,60}$' -or $id -ceq 'tunombre') {
        throw 'El identificador guardado es inválido. Vuelve a ejecutar -Modo infra -Identificador.'
    }

    $puente = @('-f', 'practica-vibracion/compose.yml')
    $topic = "$id-vibracion-eventos"

    if ($Modo -eq 'infra') {
        if (-not (Test-Path -LiteralPath 'pyspark/.env')) {
            $token = [guid]::NewGuid().ToString('N')
            Set-Content -LiteralPath 'pyspark/.env' -Value "JUPYTER_TOKEN=$token" -Encoding ascii
        }
        New-Item -ItemType Directory -Force -Path 'pyspark/sesiones/s01-arquitectura/data/2026' | Out-Null

        $plantillas = Join-Path $PSScriptRoot 'wokwi'
        $archivosWokwi = Join-Path $PSScriptRoot "wokwi-local/$id"
        New-Item -ItemType Directory -Force -Path $archivosWokwi | Out-Null
        foreach ($nombre in 'diagram.json', 'libraries.txt') {
            Copy-Item -LiteralPath (Join-Path $plantillas $nombre) -Destination $archivosWokwi
        }
        $sketch = (Get-Content -LiteralPath (Join-Path $plantillas 'sketch.ino') -Raw -Encoding utf8).Replace('"tunombre"', ('"{0}"' -f $id))
        [IO.File]::WriteAllText((Join-Path $archivosWokwi 'sketch.ino'), $sketch, [Text.UTF8Encoding]::new($false))

        $spark = @(
            '--env-file', 'pyspark/.env', '-f', 'pyspark/compose.yml',
            '-f', 'pyspark/compose.kafka.yml', '-f', 'practica-vibracion/pyspark.compose.yml'
        )
        Ejecutar-Docker compose -f kafka/compose.yml up -d --wait
        Ejecutar-Docker compose @spark up -d --wait
        Ejecutar-Docker compose -f obs/compose.yml up -d --wait
        Ejecutar-Docker compose @puente build puente

        Write-Host "Wokwi: $archivosWokwi"
        Write-Host 'Jupyter: http://localhost:4488/notebooks/practica-vibracion/vibracion.ipynb (token en pyspark/.env)'
        Write-Host 'Grafana: http://localhost:43000/d/aerostreamperu-vibracion'
        Write-Host 'En otra terminal, ejecuta .\practica-vibracion\iniciar.ps1 -Modo puente.'
        return
    }

    Ejecutar-Docker compose -f kafka/compose.yml exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists --topic $topic --partitions 1 --replication-factor 1
    $variables = @('-e', "MQTT_TOPIC=lambda26/$id/vibracion/lecturas", '-e', "KAFKA_TOPIC=$topic")
    Ejecutar-Docker compose @puente run --rm @variables puente
} finally {
    Pop-Location
}
