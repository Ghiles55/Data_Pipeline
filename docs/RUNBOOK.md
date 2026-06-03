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

## INC-04 Chaos Engineering Test Scenarios

### Scénario 1 : Arrêt d'un Broker Kafka (`docker compose stop kafka-2`)
* **Symptômes** : Avertissements dans les logs des producteurs indiquant qu'un broker est inaccessible.
* **Impact** : Aucun impact sur le flux d'ingestion. Le cluster Kafka KRaft reste opérationnel grâce au facteur de réplication de 3 et `min.insync.replicas=2`.
* **Procédure** : Redémarrer le broker avec `docker compose start kafka-2`.

### Scénario 2 : Arrêt de Spark Master (`docker compose kill spark-master`)
* **Symptômes** : Interruption des jobs Spark Structured Streaming et perte de la console de suivi Spark.
* **Impact** : Les messages s'accumulent dans Kafka. Lors du redémarrage du job Spark, la reprise se fait sans perte ni doublon grâce aux checkpoints stockés sur MinIO (`s3a://spotify-checkpoints/`).
* **Procédure** : Redémarrer le conteneur et soumettre à nouveau le job Spark.

### Scénario 3 : Arrêt temporaire PostgreSQL (`docker compose stop postgres`)
* **Symptômes** : Erreurs d'écriture JDBC dans les logs de Spark et erreurs de connexion dans Airflow.
* **Impact** : Le simulateur continue de publier dans Kafka/Redis. Dès le rétablissement de PostgreSQL, les pipelines consomment les messages accumulés. Aucune donnée n'est perdue.
* **Procédure** : Relancer PostgreSQL et s'assurer du retour à la normale des écritures.
