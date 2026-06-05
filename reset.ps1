# reset.ps1 - Reset completo do ambiente (Windows 11 / PowerShell)
# Uso: .\reset.ps1

Write-Host "🔄 Parando containers..." -ForegroundColor Yellow
docker-compose down -v --remove-orphans

Write-Host "🗑️  Limpando dados locais do MinIO..." -ForegroundColor Yellow
if (Test-Path ".\minio_data") {
    Remove-Item -Recurse -Force ".\minio_data"
}

Write-Host "🏗️  Reconstruindo e subindo containers..." -ForegroundColor Yellow
docker-compose up -d --build

Write-Host "⏳ Aguardando serviços ficarem saudáveis..." -ForegroundColor Yellow
$maxRetries = 30
$retry = 0
do {
    Start-Sleep -Seconds 5
    $retry++
    $healthy = docker-compose exec -T trino trino --execute "SELECT 1" 2>$null
    if ($LASTEXITCODE -eq 0) { break }
    Write-Host "   Tentativa $retry/$maxRetries..." -ForegroundColor Gray
} while ($retry -lt $maxRetries)

if ($retry -ge $maxRetries) {
    Write-Host "❌ Timeout aguardando serviços. Verifique: docker-compose logs" -ForegroundColor Red
    exit 1
}

Write-Host "🚀 Executando pipeline Data Vault..." -ForegroundColor Yellow
docker-compose exec dev python src/ingest_raw_datavault.py
if ($LASTEXITCODE -ne 0) {
    Write-Host "❌ Falha na ingestão." -ForegroundColor Red
    exit 1
}

docker-compose exec dev bash -c "cd dbt_datavault && dbt run --profiles-dir ."
if ($LASTEXITCODE -ne 0) {
    Write-Host "❌ Falha no dbt run." -ForegroundColor Red
    exit 1
}

docker-compose exec dev bash -c "cd dbt_datavault && dbt test --profiles-dir ."
if ($LASTEXITCODE -ne 0) {
    Write-Host "⚠️  Alguns testes falharam." -ForegroundColor Yellow
} else {
    Write-Host "✅ Pipeline Data Vault executado com sucesso!" -ForegroundColor Green
}
