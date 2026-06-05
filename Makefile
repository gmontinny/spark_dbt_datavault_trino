# The name of the Docker Compose file
COMPOSE_FILE = docker-compose.yml

.PHONY: help up down rebuild logs browse ml-demo analise otimizacao ml-all ingest dbt-run dbt-test

help: ## Show this help message
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Targets:'
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

# Run the containers in the background
up: ## Start development environment
	docker-compose -f $(COMPOSE_FILE) up -d

# Stop the containers
down: ## Stop containers
	docker-compose -f $(COMPOSE_FILE) down

# Rebuild the Docker image and restart the containers
rebuild: down up ## Rebuild and restart containers

# Show logs
logs: ## Show container logs
	docker-compose -f $(COMPOSE_FILE) logs

# Open the browser
browse: ## Open Spark UI in browser
	open http://localhost:4040

# Machine Learning targets
ml-demo: ## Execute complete ML pipeline demo
	docker-compose exec dev python src/ml_pipeline_demo.py

analise: ## Run exploratory data analysis
	docker-compose exec dev python src/analise_exploratoria.py

otimizacao: ## Run model optimization and comparison
	docker-compose exec dev python src/otimizacao_modelo.py

ml-all: ## Run all ML scripts in sequence
	make analise
	make ml-demo
	make otimizacao

teste: ## Quick environment test
	docker-compose exec dev python src/teste_rapido.py

# Data Pipeline: Spark ingest + dbt transform
ingest: ## Ingest CSV files into raw tables via Spark
	docker-compose exec dev python src/migrate_csv_to_trino.py

dbt-run: ## Run dbt models (staging + marts)
	docker-compose exec dev bash -c "cd dbt_olist && dbt run --profiles-dir ."

dbt-test: ## Run dbt tests
	docker-compose exec dev bash -c "cd dbt_olist && dbt test --profiles-dir ."

pipeline: ingest dbt-run ## Full pipeline: ingest raw + dbt transform

# Data Vault Pipeline
dv-ingest: ## Ingest CSV into raw Iceberg table for Data Vault
	docker-compose exec dev python src/ingest_raw_datavault.py

dv-run: ## Run dbt Data Vault models (hubs, satellites, links)
	docker-compose exec dev bash -c "cd dbt_datavault && dbt run --profiles-dir ."

dv-test: ## Run dbt Data Vault tests
	docker-compose exec dev bash -c "cd dbt_datavault && dbt test --profiles-dir ."

dv-pipeline: dv-ingest dv-run ## Full Data Vault pipeline: ingest + transform
