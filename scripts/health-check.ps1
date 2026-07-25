# StreamMart Health Check
Write-Host "Health Check Starting..." -ForegroundColor Cyan

# Check Docker services
$services = @("kafka", "postgres", "minio", "spark-master", "spark-worker-1", "kafka-ui", "grafana")
$allHealthy = $true

foreach ($svc in $services) {
    try {
        $status = docker compose ps $svc --format json 2>$null | ConvertFrom-Json
        if ($status.State -eq "running") {
            Write-Host "OK: $svc" -ForegroundColor Green
        } else {
            Write-Host "FAIL: $svc" -ForegroundColor Red
            $allHealthy = $false
        }
    } catch {
        Write-Host "FAIL: $svc (not found)" -ForegroundColor Red
        $allHealthy = $false
    }
}

# Check Spark UI
try {
    $response = Invoke-WebRequest -Uri "http://localhost:8082" -TimeoutSec 5 -UseBasicParsing
    Write-Host "OK: Spark UI" -ForegroundColor Green
} catch {
    Write-Host "FAIL: Spark UI" -ForegroundColor Red
    $allHealthy = $false
}

# Check Kafka UI
try {
    $response = Invoke-WebRequest -Uri "http://localhost:8080" -TimeoutSec 5 -UseBasicParsing
    Write-Host "OK: Kafka UI" -ForegroundColor Green
} catch {
    Write-Host "FAIL: Kafka UI" -ForegroundColor Red
    $allHealthy = $false
}

# Summary
Write-Host ""
if ($allHealthy) {
    Write-Host "Status: HEALTHY" -ForegroundColor Green
} else {
    Write-Host "Status: DEGRADED" -ForegroundColor Yellow
}

Write-Host ""
Write-Host "URLs:"
Write-Host "  Spark UI: http://localhost:8082"
Write-Host "  Kafka UI: http://localhost:8080"
Write-Host "  MinIO:    http://localhost:9001"
Write-Host "  Grafana:  http://localhost:3000"
