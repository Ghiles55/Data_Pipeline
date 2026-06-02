# RUNBOOK

## INC-01 DAG bloqué
Symptôme: DAG running >30 min.
Procedure:
```bash
docker ps
airflow dags list-runs
```
Relancer la tâche depuis UI.

## INC-02 PostgreSQL too many connections
Cause: surcharge connexions.
Procédure: réduire concurrence, configurer pools Airflow, redémarrer postgres.

## INC-03 MinIO inaccessible
Vérifier healthcheck docker et redémarrer service.
