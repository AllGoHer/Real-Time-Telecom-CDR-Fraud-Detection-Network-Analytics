param(
    [switch]$CheckOnly
)

$ErrorActionPreference = "Stop"
$ProjectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $ProjectRoot

function Write-Step($Message) {
    Write-Host "`n=== $Message ===" -ForegroundColor Cyan
}

function Test-Container($Name) {
    $state = docker inspect $Name --format '{{.State.Running}}' 2>$null
    if ($LASTEXITCODE -ne 0) {
        throw "No se encuentra el contenedor $Name."
    }
    return ($state -eq "true")
}

function Start-ContainerIfNeeded($Name) {
    if (Test-Container $Name) {
        Write-Host "ACTIVO: $Name" -ForegroundColor Green
    }
    elseif ($CheckOnly) {
        Write-Host "DETENIDO: $Name (no se iniciará en CheckOnly)" -ForegroundColor Yellow
    }
    else {
        Write-Host "Iniciando contenedor: $Name" -ForegroundColor Yellow
        docker start $Name
        if ($LASTEXITCODE -ne 0) {
            throw "No se pudo iniciar $Name."
        }
    }
}

function Test-PythonScript($ScriptName) {
    $escaped = [regex]::Escape($ScriptName)
    $root = [regex]::Escape($ProjectRoot)

    $matches = @(
        Get-CimInstance Win32_Process |
        Where-Object {
            $_.CommandLine -and
            $_.CommandLine -match $escaped -and
            $_.CommandLine -match $root
        }
    )

    return ($matches.Count -gt 0)
}

function Start-PythonIfNeeded($Label, $Python, $Script) {
    if (Test-PythonScript ([IO.Path]::GetFileName($Script))) {
        Write-Host "ACTIVO: $Label" -ForegroundColor Green
        return
    }

    if ($CheckOnly) {
        Write-Host "FALTA: $Label (no se iniciará en CheckOnly)" -ForegroundColor Yellow
        return
    }

    if (-not (Test-Path $Python)) {
        throw "No existe el intérprete: $Python"
    }

    if (-not (Test-Path $Script)) {
        throw "No existe el script: $Script"
    }

    Write-Host "Iniciando: $Label" -ForegroundColor Yellow

    Start-Process -FilePath $Python `
        -ArgumentList "`"$Script`"" `
        -WorkingDirectory $ProjectRoot

    Start-Sleep -Seconds 3

    if (-not (Test-PythonScript ([IO.Path]::GetFileName($Script)))) {
        throw "No se pudo confirmar el inicio de $Label."
    }

    Write-Host "INICIADO: $Label" -ForegroundColor Green
}

Write-Step "SentinelStream: recuperación"

if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    throw "Docker no está disponible en PATH."
}

docker info *> $null
if ($LASTEXITCODE -ne 0) {
    throw "Docker Desktop no está iniciado o el motor no responde."
}

# 1. Recuperar contenedores existentes sin recrearlos.
Write-Step "Contenedores Docker"

$containers = @(
    "kafka-cdr",
    "postgres-fraud",
    "flink-jobmanager",
    "flink-taskmanager",
    "grafana-fraud"
)

foreach ($container in $containers) {
    Start-ContainerIfNeeded $container
}

if ($CheckOnly) {
    Write-Step "Comprobación integral: sin cambios"

    # Kafka
    docker exec kafka-cdr /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list *> $null
    if ($LASTEXITCODE -eq 0) {
        Write-Host "Kafka: DISPONIBLE" -ForegroundColor Green
    } else {
        Write-Host "Kafka: NO DISPONIBLE" -ForegroundColor Red
    }

    # PostgreSQL
    docker exec postgres-fraud pg_isready -U fraud_user -d fraud_detection
    if ($LASTEXITCODE -eq 0) {
        Write-Host "PostgreSQL: DISPONIBLE" -ForegroundColor Green
    } else {
        Write-Host "PostgreSQL: NO DISPONIBLE" -ForegroundColor Red
    }

    # Jobs Flink
    Write-Step "Jobs Flink"
    cmd.exe /d /c "docker exec flink-jobmanager /opt/flink/bin/flink list -r 2>&1"

    if ($LASTEXITCODE -ne 0) {
        Write-Host "Flink: NO DISPONIBLE" -ForegroundColor Red
    }

    # Procesos Python
    Write-Step "Procesos Python"

    $pythonChecks = @(
        @{ Label = "Generador CDR"; File = "cdr_generator.py" },
        @{ Label = "Consumidor V12"; File = "consumer_v12.py" },
        @{ Label = "Sink PostgreSQL"; File = "ml_predictions_to_postgres.py" }
    )

    foreach ($item in $pythonChecks) {
        if (Test-PythonScript $item.File) {
            Write-Host "$($item.Label): ACTIVO" -ForegroundColor Green
        } else {
            Write-Host "$($item.Label): FALTA" -ForegroundColor Yellow
        }
    }

    Write-Step "Comprobación terminada: no se inició ni detuvo ningún proceso"
    return
}

# 2. Esperar disponibilidad de Kafka, PostgreSQL y Flink.
Write-Step "Disponibilidad de servicios"

$checks = @(
    @{
        Label = "Kafka"
        Container = "kafka-cdr"
        Command = @("/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "localhost:9092", "--list")
    },
    @{
        Label = "PostgreSQL"
        Container = "postgres-fraud"
        Command = @("pg_isready", "-U", "fraud_user", "-d", "fraud_detection")
    }
)

foreach ($check in $checks) {
    $ready = $false

    for ($i = 1; $i -le 24; $i++) {
        docker exec $check.Container @($check.Command) *> $null

        if ($LASTEXITCODE -eq 0) {
            $ready = $true
            break
        }

        Start-Sleep -Seconds 5
    }

    if (-not $ready) {
        throw "$($check.Label) no respondió dentro del tiempo de espera."
    }

    Write-Host "$($check.Label): disponible" -ForegroundColor Green
}

$flinkReady = $false

for ($i = 1; $i -le 24; $i++) {
    cmd.exe /d /c "docker exec flink-jobmanager /opt/flink/bin/flink list -r 2>&1" > $null

    if ($LASTEXITCODE -eq 0) {
        $flinkReady = $true
        break
    }

    Start-Sleep -Seconds 5
}

if (-not $flinkReady) {
    throw "Flink no respondió."
}

Write-Host "Flink: disponible" -ForegroundColor Green

# 3. Consultar jobs Flink y enviar únicamente los ausentes.
Write-Step "Jobs Flink"

$jobs = @(
    @{ Name = "sentinelstream-bronze-cdr"; File = "bronze_ingestion.py" },
    @{ Name = "sentinelstream-fraud-alerts"; File = "fraud_alerts.py" },
    @{ Name = "sentinelstream-gold-metrics"; File = "gold_layer.py" }
)

foreach ($job in $jobs) {
    $runningJobs = cmd.exe /d /c "docker exec flink-jobmanager /opt/flink/bin/flink list -r 2>&1" | Out-String

    if ($LASTEXITCODE -ne 0) {
        throw "No se pudo consultar el estado de Flink."
    }

    if ($runningJobs -match [regex]::Escape($job.Name)) {
        Write-Host "ACTIVO: $($job.Name)" -ForegroundColor Green
    }
    else {
        Write-Host "Iniciando job ausente: $($job.Name)" -ForegroundColor Yellow

        cmd.exe /d /c "docker exec flink-jobmanager /opt/flink/bin/flink run -d -py /opt/flink/jobs/$($job.File) 2>&1"

        if ($LASTEXITCODE -ne 0) {
            throw "Falló el inicio de $($job.Name)."
        }
    }
}

# 4. Recuperar procesos Python de la arquitectura V12.
Write-Step "Procesos Python V12"

$producerPython = Join-Path $ProjectRoot "src\producers\venv\Scripts\python.exe"
$inferencePython = Join-Path $ProjectRoot "src\inference\venv\Scripts\python.exe"

Start-PythonIfNeeded "Generador CDR" `
    $producerPython `
    (Join-Path $ProjectRoot "src\producers\cdr_generator.py")

Start-PythonIfNeeded "Consumidor ML V12" `
    $inferencePython `
    (Join-Path $ProjectRoot "src\inference\consumer_v12.py")

Start-PythonIfNeeded "Sink PostgreSQL" `
    $inferencePython `
    (Join-Path $ProjectRoot "src\inference\ml_predictions_to_postgres.py")

Write-Step "Recuperación completada"

Write-Host "Grafana: http://localhost:3000"
Write-Host "Flink:   http://localhost:8081"
