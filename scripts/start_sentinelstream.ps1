$ErrorActionPreference = "Stop"

Write-Host "`n=== SentinelStream: inicio del sistema ===" -ForegroundColor Cyan

# 1. Iniciar servicios sin eliminar datos ni volúmenes
docker compose up -d
if ($LASTEXITCODE -ne 0) {
    throw "No se pudieron iniciar los servicios Docker."
}

# 2. Esperar hasta que Flink responda
$flinkReady = $false

for ($attempt = 1; $attempt -le 12; $attempt++) {

    Write-Host "Comprobando Flink (intento $attempt de 12)..." -ForegroundColor Yellow

    docker exec flink-jobmanager /opt/flink/bin/flink list -r

    if ($LASTEXITCODE -eq 0) {
        $flinkReady = $true
        Write-Host "Flink está respondiendo correctamente." -ForegroundColor Green
        break
    }

    Start-Sleep -Seconds 5
}

if (-not $flinkReady) {
    throw "Flink no respondió después de varios intentos. Revisa el contenedor flink-jobmanager."
}

# 3. Iniciar solamente los trabajos que no estén activos
$jobs = @(
    @{ Name = "sentinelstream-bronze-cdr"; File = "bronze_ingestion.py" },
    @{ Name = "sentinelstream-fraud-alerts"; File = "fraud_alerts.py" },
    @{ Name = "sentinelstream-gold-metrics"; File = "gold_layer.py" }
)

foreach ($job in $jobs) {

    $runningJobs = docker exec flink-jobmanager /opt/flink/bin/flink list -r 2>&1 | Out-String

    if ($LASTEXITCODE -ne 0) {
        throw "No se pudo consultar el estado de los trabajos Flink."
    }

    if ($runningJobs -match [regex]::Escape($job.Name)) {
        Write-Host "ACTIVO: $($job.Name)" -ForegroundColor Green
    }
    else {
        Write-Host "Iniciando: $($job.File)" -ForegroundColor Yellow

        docker exec flink-jobmanager /opt/flink/bin/flink run -d -py "/opt/flink/jobs/$($job.File)"

        if ($LASTEXITCODE -ne 0) {
            throw "Falló el inicio de $($job.File)."
        }
    }
}

# 4. Detectar si el generador ya está ejecutándose
$pythonProcesses = @(
    Get-CimInstance Win32_Process |
        Where-Object {
            $_.CommandLine -and
            $_.CommandLine.Contains("cdr_generator.py")
        }
)

if ($pythonProcesses.Count -gt 0) {
    Write-Host "El generador CDR ya está ejecutándose; no se iniciará otra instancia." -ForegroundColor Green
}
else {
    $python = Join-Path $PWD ".venv-producer\Scripts\python.exe"
    $producer = Join-Path $PWD "src\producers\cdr_generator.py"

    if (-not (Test-Path $python)) {
        throw "No se encuentra el Python del entorno virtual: $python"
    }

    if (-not (Test-Path $producer)) {
        throw "No se encuentra el generador: $producer"
    }

    Start-Process -FilePath $python `
        -ArgumentList "`"$producer`"" `
        -WorkingDirectory $PWD

    Write-Host "Generador CDR iniciado." -ForegroundColor Green
}

Write-Host "`n=== SentinelStream: proceso de inicio completado ===" -ForegroundColor Cyan
Write-Host "Grafana: http://localhost:3000"
Write-Host "Flink:   http://localhost:8081"
