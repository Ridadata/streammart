# ============================================================================
# StreamMart Startup Script
# Brings up the entire pipeline in the correct order
# ============================================================================

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  StreamMart Pipeline Startup"  -ForegroundColor Cyan
Write-Host "========================================`n" -ForegroundColor Cyan

# Check Docker is running
Write-Host "[1/8] Checking Docker..." -ForegroundColor Yellow
$dockerRunning = docker info 2>$null
if (-not $dockerRunning) {
    Write-Host "✗ Docker is not running. Please start Docker Desktop first." -ForegroundColor Red
    exit 1
}
Write-Host "✓ Docker is running`n" -ForegroundColor Green

# Check if .env file exists
Write-Host "[2/8] Checking environment file..." -ForegroundColor Yellow
if (-not (Test-Path ".env")) {
    Write-Host "✗ .env file not found!" -ForegroundColor Red
    exit 1
}
Write-Host "✓ Environment file found`n" -ForegroundColor Green

# Build and start services
Write-Host "[3/8] Starting infrastructure services..." -ForegroundColor Yellow
Write-Host "This will take 2-3 minutes on first run...`n" -ForegroundColor Gray

docker compose up -d kafka schema-registry postgres minio prometheus grafana
if ($LASTEXITCODE -ne 0) {
    Write-Host "✗ Failed to start infrastructure services" -ForegroundColor Red
    exit 1
}
Write-Host "✓ Infrastructure services started`n" -ForegroundColor Green

# Wait for Kafka to be ready
Write-Host "[4/8] Waiting for Kafka to be ready (60s)..." -ForegroundColor Yellow
Start-Sleep -Seconds 60
Write-Host "✓ Kafka should be ready`n" -ForegroundColor Green

# Initialize Kafka topics
Write-Host "[5/8] Creating Kafka topics..." -ForegroundColor Yellow
docker compose exec -T kafka bash /scripts/init_kafka_topics.sh
if ($LASTEXITCODE -ne 0) {
    Write-Host "⚠ Topic creation had issues (may already exist)" -ForegroundColor Yellow
}
Write-Host "✓ Topics initialized`n" -ForegroundColor Green

# Start Spark and Airflow
Write-Host "[6/8] Starting Spark and Airflow..." -ForegroundColor Yellow
docker compose up -d spark-master spark-worker-1 airflow-webserver airflow-scheduler kafka-ui postgres-exporter
if ($LASTEXITCODE -ne 0) {
    Write-Host "✗ Failed to start processing services" -ForegroundColor Red
    exit 1
}
Write-Host "✓ Processing services started`n" -ForegroundColor Green

# Wait for services to stabilize
Write-Host "[7/8] Waiting for services to stabilize (30s)..." -ForegroundColor Yellow
Start-Sleep -Seconds 30
Write-Host "✓ Services should be ready`n" -ForegroundColor Green

# Show service status
Write-Host "[8/8] Checking service health..." -ForegroundColor Yellow
docker compose ps

Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "  StreamMart is ready!" -ForegroundColor Green
Write-Host "========================================`n" -ForegroundColor Cyan

Write-Host "Access the services:" -ForegroundColor White
Write-Host "  • Kafka UI:        http://localhost:8080" -ForegroundColor Gray
Write-Host "  • MinIO Console:   http://localhost:9001 (admin/minioadmin)" -ForegroundColor Gray
Write-Host "  • Grafana:         http://localhost:3000 (admin/admin)" -ForegroundColor Gray
Write-Host "  • Airflow:         http://localhost:8085 (admin/admin)" -ForegroundColor Gray
Write-Host "  • Prometheus:      http://localhost:9090`n" -ForegroundColor Gray

Write-Host "Next steps:" -ForegroundColor White
Write-Host "  1. Install Python dependencies: pip install -r requirements.txt" -ForegroundColor Gray
Write-Host "  2. Start event generator: python src/simulator/event_generator.py --rate 100" -ForegroundColor Gray
Write-Host "  3. Start Spark jobs: See README for spark-submit commands" -ForegroundColor Gray
Write-Host "  4. Monitor in Grafana: http://localhost:3000`n" -ForegroundColor Gray

Write-Host "To stop all services: docker compose down" -ForegroundColor Yellow
Write-Host "To stop and remove data: docker compose down -v`n" -ForegroundColor Yellow
