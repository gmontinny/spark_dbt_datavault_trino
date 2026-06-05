#!/usr/bin/env bash
# reset.sh - Reset completo do ambiente (Linux / macOS)
# Uso: chmod +x reset.sh && ./reset.sh

set -e

echo "🔄 Parando containers..."
docker-compose down -v --remove-orphans

echo "🗑️  Limpando dados locais do MinIO..."
rm -rf ./minio_data

echo "🏗️  Reconstruindo e subindo containers..."
docker-compose up -d --build

echo "⏳ Aguardando serviços ficarem saudáveis..."
MAX_RETRIES=30
RETRY=0
until docker-compose exec -T trino trino --execute "SELECT 1" &>/dev/null; do
    RETRY=$((RETRY + 1))
    if [ "$RETRY" -ge "$MAX_RETRIES" ]; then
        echo "❌ Timeout aguardando serviços. Verifique: docker-compose logs"
        exit 1
    fi
    echo "   Tentativa $RETRY/$MAX_RETRIES..."
    sleep 5
done

echo "🚀 Executando pipeline Data Vault..."
docker-compose exec dev python src/ingest_raw_datavault.py
docker-compose exec dev bash -c "cd dbt_datavault && dbt run --profiles-dir ."
docker-compose exec dev bash -c "cd dbt_datavault && dbt test --profiles-dir ."

echo "✅ Pipeline Data Vault executado com sucesso!"
