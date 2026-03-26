.PHONY: help install install-dev lint format test test-cov run-api run-ui run-producer docker-build docker-up docker-down clean

help: ## Show this help message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

install: ## Install production dependencies
	pip install -r requirements.txt

install-dev: ## Install development dependencies
	pip install -r requirements-dev.txt
	pre-commit install

lint: ## Run linting (ruff + mypy)
	ruff check src/ tests/
	mypy src/

format: ## Format code with ruff
	ruff format src/ tests/
	ruff check --fix src/ tests/

test: ## Run tests
	pytest tests/ -v

test-cov: ## Run tests with coverage report
	pytest tests/ -v --cov=src --cov-report=term-missing --cov-report=html

run-api: ## Run FastAPI server
	uvicorn src.api.routes:app --host 0.0.0.0 --port 8080 --reload

run-ui: ## Run Streamlit dashboard
	streamlit run src/ui/app.py --server.port 8501

run-producer: ## Run IoT data producer
	python -m src.ingestion.kafka_producer

run-consumer: ## Run Spark streaming consumer
	python -m src.ingestion.spark_consumer

docker-build: ## Build Docker image
	docker-compose build

docker-up: ## Start all services with Docker Compose
	docker-compose up -d

docker-down: ## Stop all services
	docker-compose down -v

clean: ## Clean build artifacts
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
	rm -rf .ruff_cache .mypy_cache htmlcov .coverage dist build *.egg-info
	rm -rf spark-warehouse metastore_db derby.log
