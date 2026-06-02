# Data Pipeline

## Architecture

```text
Data Generator -> Airflow DAGs -> PostgreSQL -> Redis / MinIO -> Dashboard
```

## Run
```bash
docker compose up -d
pytest tests/ -v --tb=short
```
