# 🚀 Procédure de Test Individuel des Tâches (#1 à #25)

Ce document fournit la procédure pas-à-pas pour valider individuellement chacune des 25 tâches (issues GitHub) du projet **Spotify Data Pipeline** sur la branche `test_phase3`.

---

## 📊 Phase 1 — Batch Layer (Issues #1 à #10)

### #1 — Setup Docker Compose et vérification de la stack
* **Fichier associé** : [docker-compose.yml](file:///d:/Projects/Data_Pipeline/docker-compose.yml)
* **Commande** :
  ```bash
  docker compose ps
  ```
* **Vérification** :
  - Tous les conteneurs sont en statut `Up` (ou `healthy`).
  - L'interface Airflow est accessible sur [http://localhost:8080](http://localhost:8080).
  - L'interface MinIO est accessible sur [http://localhost:9001](http://localhost:9001).
  - L'interface Kafka UI est accessible sur [http://localhost:8090](http://localhost:8090).

### #2 — Schéma PostgreSQL et modèle de données
* **Fichier associé** : [init_spotify_db.sql](file:///d:/Projects/Data_Pipeline/sql/init_spotify_db.sql)
* **Commande** :
  ```bash
  docker compose exec postgres psql -U spotify -d spotify -c "\dt"
  ```
* **Vérification** :
  - La liste des 13 tables du modèle de données est affichée.

### #3 — Data Generator : catalogue musical avec Faker
* **Fichiers associés** : [generate_catalog.py](file:///d:/Projects/Data_Pipeline/src/data_generator/generate_catalog.py) et [upload_to_minio.py](file:///d:/Projects/Data_Pipeline/src/data_generator/upload_to_minio.py)
* **Commandes** :
  ```bash
  python -m src.data_generator.generate_catalog --artists 15
  python src/data_generator/upload_to_minio.py
  ```
* **Vérification** :
  - Ouvrez l'interface web de MinIO et vérifiez que le bucket `labels-raw` contient les trois fichiers JSON générés (`sunset_records.json`, `nightwave_music.json`, `urban_pulse.json`).

### #4 — DAG `catalog_ingestion_pipeline`
* **Fichier associé** : [catalog_ingestion_pipeline.py](file:///d:/Projects/Data_Pipeline/dags/catalog_ingestion_pipeline.py)
* **Actions** :
  1. Déclenchez manuellement le DAG `catalog_ingestion_pipeline` depuis l'UI Airflow.
  2. Lancez la requête suivante dans PostgreSQL :
     ```bash
     docker compose exec postgres psql -U spotify -d spotify -c "SELECT COUNT(*) FROM tracks;"
     ```
* **Vérification** :
  - Le DAG s'exécute avec succès (toutes les tâches en vert).
  - La table `tracks` contient 1 362 morceaux.
  - Relancez le DAG une seconde fois : le compteur de morceaux reste à 1 362 (idempotence).

### #5 — Simulateur P2P : compléter et lancer
* **Fichier associé** : [simulator.py](file:///d:/Projects/Data_Pipeline/src/p2p_simulator/simulator.py)
* **Actions** :
  1. Lancez le simulateur en local :
     ```bash
     python -m src.p2p_simulator.simulator --peers 10 --rate 3
     ```
  2. Dans une autre console, observez le contenu de Redis :
     ```bash
     docker compose exec redis redis-cli -n 1 llen listening_events_list
     ```
* **Vérification** :
  - Le simulateur s'exécute en continu sans crash et produit des logs de publication.
  - La taille de la liste dans Redis augmente en continu.

### #6 — DAG `streaming_events_pipeline`
* **Fichier associé** : [streaming_events_pipeline.py](file:///d:/Projects/Data_Pipeline/dags/streaming_events_pipeline.py)
* **Actions** :
  1. Laissez tourner le simulateur quelques instants pour accumuler des messages.
  2. Déclenchez le DAG `streaming_events_pipeline` sur Airflow.
  3. Lancez les commandes SQL et de fichiers :
     ```bash
     docker compose exec postgres psql -U spotify -d spotify -c "SELECT COUNT(*) FROM listening_events;"
     ```
* **Vérification** :
  - La table `listening_events` de PostgreSQL s'alimente.
  - Dans l'interface MinIO, des fichiers Parquet partitionnés sont présents dans le bucket `spotify-parquet` sous `listening_events/date=.../hour=.../`.

### #7 — DAG `aggregation_pipeline`
* **Fichier associé** : [aggregation_pipeline.py](file:///d:/Projects/Data_Pipeline/dags/aggregation_pipeline.py)
* **Actions** :
  1. Déclenchez le DAG `aggregation_pipeline` sur Airflow.
  2. Interrogez la table d'agrégats :
     ```bash
     docker compose exec postgres psql -U spotify -d spotify -c "SELECT * FROM daily_streams ORDER BY total_streams DESC LIMIT 5;"
     ```
* **Vérification** :
  - Les tables `daily_streams` et `artist_stats` contiennent les indicateurs de streams consolidés.

### #8 — DAG `recommendation_pipeline`
* **Fichier associé** : [recommendation_pipeline.py](file:///d:/Projects/Data_Pipeline/dags/recommendation_pipeline.py)
* **Actions** :
  1. Déclenchez le DAG `recommendation_pipeline` sur Airflow.
  2. Vérifiez la présence des recommandations en base et dans le cache Redis :
     ```bash
     docker compose exec postgres psql -U spotify -d spotify -c "SELECT * FROM recommendations LIMIT 5;"
     docker compose exec redis redis-cli -n 1 keys "reco:*"
     ```
* **Vérification** :
  - PostgreSQL contient les listes de recommandations par utilisateur.
  - Redis stocke les clés de recommandation avec un format JSON de type track_id et un TTL de 24h.

### #9 — Représentation DLQ & Reprocessing DAG
* **Fichier associé** : [dlq_reprocessing_pipeline.py](file:///d:/Projects/Data_Pipeline/dags/dlq_reprocessing_pipeline.py)
* **Actions** :
  1. Injectez un événement correctable (sans timestamp) et un incorrigible (sans `user_id`) dans la table DLQ :
     ```bash
     # Sous PowerShell :
     Get-Content data/test_dlq_injection.sql | docker compose exec -T postgres psql -U spotify -d spotify
     ```
  2. Déclenchez le DAG `dlq_reprocessing_pipeline` sur Airflow.
  3. Observez l'état de la DLQ :
     ```bash
     docker compose exec postgres psql -U spotify -d spotify -c "SELECT id, status, retry_count FROM dead_letter_events;"
     ```
* **Vérification** :
  - Le premier événement passe en statut `reprocessed` et est inséré dans `listening_events`.
  - Le second événement reste `pending` et son `retry_count` passe à `1`. Après 3 lancements de DAG, son statut devient `abandoned`.

### #10 — Tests unitaires & Qualité de code
* **Actions** :
  1. Lancez la suite complète de tests dans le conteneur du scheduler :
     ```bash
     docker compose exec airflow-scheduler python -m pytest tests/ -v
     ```
* **Vérification** :
  - Tous les 35 tests passent avec succès (`35 passed`).

---

## 📡 Phase 2 — Streaming & Temps Réel

### #11 — Cluster Kafka KRaft dans docker-compose
* **Vérification** :
  - Accédez à Kafka UI ([http://localhost:8090](http://localhost:8090)) et assurez-vous que les 6 topics internes sont présents avec la configuration attendue (ex: topic `listening_events` à 6 partitions et facteur de réplication 3).

### #12 — Migration simulateur P2P vers Kafka
* **Fichier associé** : [simulator.py](file:///d:/Projects/Data_Pipeline/src/p2p_simulator/simulator.py)
* **Actions** :
  1. Démarrez le simulateur en local.
  2. Ouvrez Kafka UI, naviguez sur le topic `listening_events` et allez dans l'onglet **Messages**.
* **Vérification** :
  - Les événements d'écoutes arrivent en temps réel dans toutes les partitions du topic.

### #13 — Premier job Spark : lecture et console
* **Fichier associé** : [streaming_trends_job.py](file:///d:/Projects/Data_Pipeline/spark_jobs/streaming_trends_job.py)
* **Commande** :
  ```bash
  docker compose exec -u root spark-master /opt/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.1,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 /opt/spark-jobs/streaming_trends_job.py --mode console --trigger once
  ```
* **Vérification** :
  - Les logs Spark affichent un tableau structuré (contenant `event_time`) issu du flux Kafka.

### #14 — Job `streaming_trends_job` : fenêtres temporelles
* **Actions** :
  1. Lancez le job d'analyse de tendances Spark en tâche de fond :
     ```bash
     docker compose exec -d -u root spark-master /opt/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.1,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 /opt/spark-jobs/streaming_trends_job.py --mode trends
     ```
  2. Lancez le simulateur et observez les sorties :
     ```bash
     docker compose exec postgres psql -U spotify -d spotify -c "SELECT * FROM realtime_top_tracks ORDER BY stream_count DESC LIMIT 5;"
     docker compose exec redis redis-cli -n 1 hgetall genre_listeners:live
     ```
* **Vérification** :
  - Les écoutes temps réel par tranches de 5 min s'écrivent dans Postgres.
  - Les statistiques glissantes par genre musical se mettent à jour dans Redis.

### #15 — Watermarking et gestion des late events
* **Fichier associé** : [streaming_trends_job.py](file:///d:/Projects/Data_Pipeline/spark_jobs/streaming_trends_job.py)
* **Actions** :
  1. Lancez le simulateur en mode retard :
     ```bash
     python -m src.p2p_simulator.simulator --mode late_events --rate 15 --peers 5
     ```
  2. Observez le topic des retards :
     ```bash
     docker compose exec kafka-1 /usr/bin/kafka-get-offsets --bootstrap-server kafka-1:9092 --topic late_listening_events
     ```
* **Vérification** :
  - Les événements avec plus de 10 min de retard sont détectés par le watermark de Spark et écrits dans le topic `late_listening_events`.

### #16 — Exactly-once semantics bout-en-bout
* **Vérification** :
  - Lancez la requête de décompte de doublons :
    ```bash
    docker compose exec postgres psql -U spotify -d spotify -c "SELECT COUNT(*) - COUNT(DISTINCT id) AS doublons FROM listening_events;"
    ```
  - Le résultat retourné doit rester égal à `0` (garanti par le mode transactionnel de Kafka et les contraintes d'unicité PostgreSQL).

### #17 — Job `streaming_enrichment_job`
* **Fichier associé** : [streaming_enrichment_job.py](file:///d:/Projects/Data_Pipeline/spark_jobs/streaming_enrichment_job.py)
* **Actions** :
  1. Soumettez le job d'enrichissement en continu :
     ```bash
     docker compose exec -d -u root spark-master /opt/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.1,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 /opt/spark-jobs/streaming_enrichment_job.py
     ```
  2. Observez le topic Kafka `enriched_events` dans l'interface Kafka UI.
* **Vérification** :
  - Le topic `enriched_events` reçoit les écoutes enrichies à la volée avec l'artiste et le genre musical.
  - Le bucket MinIO `spotify-parquet/enriched/` stocke les fichiers Parquet partitionnés.

### #18 — Job `fraud_detection_job`
* **Fichier associé** : [fraud_detection_job.py](file:///d:/Projects/Data_Pipeline/spark_jobs/fraud_detection_job.py)
* **Actions** :
  1. Lancez le job de détection de fraudes :
     ```bash
     docker compose exec -d -u root spark-master /opt/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.1,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 /opt/spark-jobs/fraud_detection_job.py > fraud_job.log 2>&1
     ```
  2. Lancez le simulateur en mode fraude pour alimenter le topic :
     ```bash
     python -m src.p2p_simulator.simulator --mode fraud --rate 20 --peers 5
     ```
  3. Vérifiez les alertes stockées en base et dans Kafka :
     ```bash
     docker compose exec postgres psql -U spotify -d spotify -c "SELECT COUNT(*), fraud_type FROM fraud_detections GROUP BY fraud_type;"
     docker compose exec kafka-1 /usr/bin/kafka-get-offsets --bootstrap-server kafka-1:9092 --topic fraud_alerts
     ```
* **Vérification** :
  - Les alertes de type `burst_listen` et `bad_peer` sont calculées en continu et enregistrées.
  - Le topic Kafka `fraud_alerts` reçoit les alertes.

### #19 — DAG `reconciliation_pipeline`
* **Fichier associé** : [reconciliation_pipeline.py](file:///d:/Projects/Data_Pipeline/dags/reconciliation_pipeline.py)
* **Actions** :
  1. Déclenchez le DAG `reconciliation_pipeline` sur Airflow.
  2. Observez les données insérées :
     ```bash
     docker compose exec postgres psql -U spotify -d spotify -c "SELECT * FROM reconciliation_reports LIMIT 10;"
     ```
* **Vérification** :
  - La table `reconciliation_reports` est alimentée avec les métriques et taux de divergence entre Batch et Speed Layer.

### #20 — DAG `late_events_reprocessing`
* **Fichier associé** : [late_events_reprocessing.py](file:///d:/Projects/Data_Pipeline/dags/late_events_reprocessing.py)
* **Actions** :
  1. Laissez le simulateur tourner en mode `late_events` pour remplir le topic.
  2. Déclenchez le DAG `late_events_reprocessing` sur Airflow.
  3. Observez la table `listening_events` :
     ```bash
     docker compose exec postgres psql -U spotify -d spotify -c "SELECT count(*) FROM listening_events WHERE event_source = 'p2p';"
     ```
* **Vérification** :
  - Le nombre d'écoutes augmente lors de l'exécution, confirmant que les événements tardifs Kafka ont été réinsérés rétroactivement.

---

## 🔗 Phase 3 — Interconnexion inter-groupes

### #21 — Data contracts inter-groupes
* **Vérification** :
  - Assurez-vous de la présence des schémas de données validés dans le dossier `contracts/` (ex. `catalog_federation_schema.json`).

### #22 — DAG `catalog_federation_pipeline`
* **Fichier associé** : [catalog_federation_pipeline.py](file:///d:/Projects/Data_Pipeline/dags/catalog_federation_pipeline.py)
* **Actions** :
  1. Déclenchez le DAG `catalog_federation_pipeline` sur Airflow.
  2. Observez les messages sur le topic :
     ```bash
     docker compose exec kafka-1 /usr/bin/kafka-get-offsets --bootstrap-server kafka-1:9092 --topic catalog_federation
     ```
* **Vérification** :
  - Les 1 362 morceaux du catalogue local ont été écrits sur le topic Kafka de fédération partagé.

### #23 — P2P cross-group
* **Fichier associé** : [simulator.py](file:///d:/Projects/Data_Pipeline/src/p2p_simulator/simulator.py)
* **Vérification** :
  - Lancez le simulateur en local et observez sa console. Des messages du type `[CROSS-GROUP] groupe-a → groupe-b : track_id=... latency=...ms OK` s'affichent lors des échanges simulés.

### #24 — Top 50 Global SPOTIFY
* **Fichier associé** : [global_top50_pipeline.py](file:///d:/Projects/Data_Pipeline/dags/global_top50_pipeline.py)
* **Actions** :
  1. Déclenchez le DAG `global_top50_pipeline` sur Airflow.
  2. Interrogez la clé finale dans Redis :
     ```bash
     docker compose exec redis redis-cli -n 1 get top50:global
     ```
* **Vérification** :
  - Redis retourne une chaîne JSON valide contenant les 50 morceaux les plus écoutés de manière agrégée à l'échelle globale.

### #25 — Chaos engineering + documentation finale
* **Actions** :
  1. Vérifiez une dernière fois que la couverture de tests est complète :
     ```bash
     docker compose exec airflow-scheduler python -m pytest tests/ -v
     ```
* **Vérification** :
  - `35 passed`. La documentation finale dans `docs/RUNBOOK.md` et `README.md` est à jour.
