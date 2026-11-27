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

Push-Location -LiteralPath $PSScriptRoot
try {
    $sesion = 'pyspark/sesiones/examen-temperatura'
    $config = Join-Path $sesion 'identificador.local'
    if ($Modo -eq 'puente' -and $Identificador) {
        throw 'Configura el identificador primero con -Modo infra -Identificador.'
    }
    if ($Identificador) {
        Set-Content -LiteralPath $config -Value $Identificador -Encoding ascii
    }
    if (-not (Test-Path -LiteralPath $config -PathType Leaf)) {
        throw 'Ejecuta primero -Modo infra -Identificador con tu nombre y apellido en minúsculas, sin tildes ni espacios.'
    }
    $id = (Get-Content -LiteralPath $config -Raw).Trim()
    if ($id -cnotmatch '^[a-z]{2,60}$' -or $id -ceq 'tunombre') {
        throw 'El identificador guardado es inválido. Vuelve a ejecutar -Modo infra -Identificador.'
    }

    $composePuente = @('-f', 'uso-atmos-examen/compose.yml')
    $topic = "$id-temperatura-eventos"

    if ($Modo -eq 'infra') {
        if (-not (Test-Path -LiteralPath 'pyspark/.env')) {
            $token = [guid]::NewGuid().ToString('N')
            Set-Content -LiteralPath 'pyspark/.env' -Value "JUPYTER_TOKEN=$token" -Encoding ascii
        }
        New-Item -ItemType Directory -Force -Path 'pyspark/sesiones/s01-arquitectura/data/2026' | Out-Null

        $plantillas = 'uso-atmos-examen/wokwi'
        $local = "uso-atmos-examen/wokwi-local/$id"
        New-Item -ItemType Directory -Force -Path $local | Out-Null
        foreach ($nombre in 'diagram.json', 'libraries.txt') {
            Copy-Item -LiteralPath (Join-Path $plantillas $nombre) -Destination $local
        }
        $sketch = (Get-Content -LiteralPath (Join-Path $plantillas 'sketch.ino') -Raw).Replace('"tunombre"', ('"{0}"' -f $id))
        $archivoSketch = Join-Path $local 'sketch.ino'
        [IO.File]::WriteAllText((Join-Path $PSScriptRoot $archivoSketch), $sketch, [Text.UTF8Encoding]::new($false))

        & ./iniciar.ps1 infra
        Ejecutar-Docker compose @composePuente build uso-atmos
        Write-Host "Wokwi: $local"
        Write-Host 'Jupyter: http://localhost:4488/tree/examen-temperatura (token en pyspark/.env)'
        Write-Host 'Grafana: http://localhost:43000/d/aerostreamperu-temperatura'
        Write-Host 'En otra terminal, ejecuta .\practica-temperatura.ps1 -Modo puente.'
        return
    }

    Ejecutar-Docker compose -f kafka/compose.yml exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists --topic $topic --partitions 1 --replication-factor 1
    $variables = @(
        '-e', "MQTT_TOPIC=lambda26/$id/temperatura/lecturas",
        '-e', "KAFKA_TOPIC_ATMOS=$topic"
    )
    Ejecutar-Docker compose @composePuente run --rm @variables uso-atmos python -u bridge_mqtt_kafka.py
} finally {
    Pop-Location
}
