# StreamMart Spark Job Launcher
# Production-grade cluster mode configuration
# Run this script to start all 3 Spark streaming jobs

Write-Host "StreamMart - Starting Spark Streaming Jobs in Cluster Mode" -ForegroundColor Cyan
Write-Host ("=" * 80) -ForegroundColor Cyan
Write-Host ""

# Verify Spark cluster is running
Write-Host "Verifying Spark cluster is running..." -ForegroundColor Yellow
$sparkMaster = docker compose ps spark-master --format json | ConvertFrom-Json
if ($sparkMaster.State -ne "running") {
    Write-Host "ERROR: Spark master is not running. Start infrastructure first:" -ForegroundColor Red
    Write-Host "   docker compose up -d" -ForegroundColor White
    exit 1
}
Write-Host "OK: Spark cluster is healthy" -ForegroundColor Green
Write-Host ""

# Clear old checkpoints (optional - uncomment if needed)
# Write-Host "Clearing old checkpoints..." -ForegroundColor Yellow
# docker compose exec spark-master rm -rf /tmp/checkpoints/*
# Write-Host "Checkpoints cleared" -ForegroundColor Green
# Write-Host ""

# Start Raw Event Writer (Terminal 1)
Write-Host "Starting Job 1/3: Raw Event Writer" -ForegroundColor Cyan
Start-Sleep -Seconds 2

$job1Command = @'
Write-Host "RAW EVENT WRITER - Cluster Mode" -ForegroundColor Cyan
Write-Host "Reads from Kafka topics: events.*" -ForegroundColor Gray
Write-Host "Writes to MinIO: s3a://raw-events/events/" -ForegroundColor Gray
Write-Host ""
docker compose exec spark-master /opt/spark/bin/spark-submit `
    --master spark://spark-master:7077 `
    --deploy-mode client `
    --name "StreamMart-RawEventWriter" `
    --executor-memory 1200M `
    --executor-cores 1 `
    --total-executor-cores 1 `
    --driver-memory 512M `
    --repositories https://repo1.maven.org/maven2/ `
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.11.1026 `
    --conf spark.cores.max=1 `
    --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 `
    --conf spark.hadoop.fs.s3a.access.key=minioadmin `
    --conf spark.hadoop.fs.s3a.secret.key=minioadmin `
    --conf spark.hadoop.fs.s3a.path.style.access=true `
    --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem `
    /opt/spark-jobs/raw_event_writer.py
'@

Start-Process powershell -ArgumentList "-NoExit", "-Command", $job1Command

Start-Sleep -Seconds 20

# Start Window Aggregator (Terminal 2)
Write-Host "Starting Job 2/3: Window Aggregator" -ForegroundColor Cyan
Start-Sleep -Seconds 2

$job2Command = @'
Write-Host "WINDOW AGGREGATOR - Cluster Mode" -ForegroundColor Cyan
Write-Host "Reads from Kafka topics: events.*" -ForegroundColor Gray
Write-Host "Writes to PostgreSQL: metrics_1min table" -ForegroundColor Gray
Write-Host ""
docker compose exec spark-master /opt/spark/bin/spark-submit `
    --master spark://spark-master:7077 `
    --deploy-mode client `
    --name "StreamMart-WindowAggregator" `
    --executor-memory 1200M `
    --executor-cores 1 `
    --total-executor-cores 1 `
    --driver-memory 512M `
    --repositories https://repo1.maven.org/maven2/ `
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.6.0 `
    --conf spark.cores.max=1 `
    /opt/spark-jobs/window_aggregator.py
'@

Start-Process powershell -ArgumentList "-NoExit", "-Command", $job2Command

Start-Sleep -Seconds 20

# Start Session Tracker (Terminal 3)
Write-Host "Starting Job 3/3: Session Tracker" -ForegroundColor Cyan
Start-Sleep -Seconds 2

$job3Command = @'
Write-Host "SESSION TRACKER - Cluster Mode" -ForegroundColor Cyan
Write-Host "Reads from Kafka topics: events.*" -ForegroundColor Gray
Write-Host "Writes to PostgreSQL: session_summary table" -ForegroundColor Gray
Write-Host ""
docker compose exec spark-master /opt/spark/bin/spark-submit `
    --master spark://spark-master:7077 `
    --deploy-mode client `
    --name "StreamMart-SessionTracker" `
    --executor-memory 1200M `
    --executor-cores 1 `
    --total-executor-cores 1 `
    --driver-memory 512M `
    --repositories https://repo1.maven.org/maven2/ `
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.6.0 `
    --conf spark.cores.max=1 `
    /opt/spark-jobs/session_tracker.py
'@

Start-Process powershell -ArgumentList "-NoExit", "-Command", $job3Command

Start-Sleep -Seconds 10

Write-Host ""
Write-Host ("=" * 80) -ForegroundColor Cyan
Write-Host "SUCCESS: All 3 Spark jobs launched in separate terminals!" -ForegroundColor Green
Write-Host ""
Write-Host "Monitor jobs at:" -ForegroundColor Yellow
Write-Host "   Spark UI:   http://localhost:8082" -ForegroundColor White
Write-Host "   Kafka UI:   http://localhost:8080" -ForegroundColor White
Write-Host ""
Write-Host "Expected in Spark UI:" -ForegroundColor Yellow
Write-Host "   - 3 Running Applications" -ForegroundColor White
Write-Host "   - Each with 1 core (3 cores total, 3 cores free)" -ForegroundColor White
Write-Host "   - Each with 1200M executor memory (3600M total)" -ForegroundColor White
Write-Host ""
Write-Host "Keep all 3 terminal windows running!" -ForegroundColor Red
Write-Host ""
