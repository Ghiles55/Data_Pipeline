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

## INC-04 Chaos Engineering Test Scenarios

### Scénario 1 : Arrêt d'un Broker Kafka (`docker compose stop kafka-2`)
* **Symptômes** : Avertissements dans les logs des producteurs indiquant qu'un broker est inaccessible.
* **Impact** : Aucun impact sur le flux d'ingestion. Le cluster Kafka KRaft reste opérationnel grâce au facteur de réplication de 3 et `min.insync.replicas=2`. Les messages continuent d'être produits et consommés.
* **Résultat du test** : Succès. La haute disponibilité est assurée.
* **Procédure de remédiation** : 
  1. Identifier la cause de l'arrêt (OOM, disque plein).
  2. Relancer le broker avec `docker compose start kafka-2`.
  3. Vérifier que la réplication se synchronise via l'UI Kafka.

### Scénario 2 : Arrêt de Spark Master (`docker compose kill spark-master`)
* **Symptômes** : Interruption des jobs Spark Structured Streaming et perte de la console de suivi Spark.
* **Impact** : Les messages s'accumulent dans Kafka. Les tables Postgres temps réel (`realtime_top_tracks`, `fraud_detections`) ne sont plus mises à jour.
* **Résultat du test** : Succès. Lors du redémarrage du job Spark, la reprise se fait sans perte ni doublon grâce aux checkpoints stockés sur MinIO (`s3a://spotify-checkpoints/`). Le retard est rattrapé par les micro-batchs.
* **Procédure de remédiation** : 
  1. Redémarrer le conteneur avec `docker compose start spark-master`.
  2. Soumettre à nouveau le job Spark via `spark-submit`.
  3. Surveiller le lag Kafka pour s'assurer que le retard est absorbé.

### Scénario 3 : Arrêt temporaire PostgreSQL (`docker compose stop postgres`)
* **Symptômes** : Erreurs d'écriture JDBC dans les logs de Spark, erreurs de connexion dans Airflow, et échec des requêtes de l'UI.
* **Impact** : Le simulateur continue de publier dans Kafka/Redis de manière transparente. Les jobs Spark échouent et retentent. Airflow met les tâches en échec.
* **Résultat du test** : Succès. Dès le rétablissement de PostgreSQL, les pipelines consomment les messages accumulés dans Kafka. La DLQ capture les événements échoués durant la panne, qui sont retraités ensuite par Airflow. Aucune donnée n'est perdue.
* **Procédure de remédiation** : 
  1. Relancer PostgreSQL avec `docker compose start postgres`.
  2. Relancer les tâches Airflow échouées (clear failures).
  3. Vérifier le bon retraitement via la DLQ.
