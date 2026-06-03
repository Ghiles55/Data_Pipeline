"""
DAG : late_events_reprocessing
==================================
Consomme le topic Kafka `late_listening_events` pour réintégrer les événements tardifs
qui ont été routés et ignorés par Spark Streaming. Réinitialise les agrégats batch.
"""

from datetime import datetime, timedelta
import json
import logging

from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

DAG_DOC = """
## late_events_reprocessing

### Rôle
Consomme et réintègre les événements d'écoute tardifs du topic Kafka `late_listening_events`.
Recalcule les statistiques quotidiennes (`daily_streams`) pour les dates affectées.

### Architecture Lambda
- **Speed Layer** : Spark détecte les late events (>10 min par rapport au watermark) et les route vers Kafka.
- **Batch Layer** : Ce DAG (exécuté toutes les heures) traite et recalcule à la volée.
"""

DEFAULT_ARGS = {
    "owner":             "spotify-team",
    "depends_on_past":   False,
    "start_date":        datetime(2025, 1, 1),
    "retries":           1,
    "retry_delay":       timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=20),
}

POSTGRES_CONN_ID = "spotify_postgres"
KAFKA_BOOTSTRAP  = "kafka-1:9092,kafka-2:9094,kafka-3:9096"
TOPIC            = "late_listening_events"

with DAG(
    dag_id="late_events_reprocessing",
    default_args=DEFAULT_ARGS,
    description="Retraitement horaire des événements d'écoute tardifs",
    schedule_interval="@hourly",
    catchup=False,
    max_active_runs=1,
    tags=["spotify", "phase-3", "late-events", "lambda"],
    doc_md=DAG_DOC,
) as dag:

    @task(task_id="consume_and_insert_late_events")
    def consume_and_insert_late_events() -> list:
        logger = logging.getLogger(__name__)
        from confluent_kafka import Consumer, KafkaError
        
        # Initialisation du consommateur Kafka
        conf = {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "group.id":          "late_events_airflow_consumer",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False
        }
        
        consumer = Consumer(conf)
        consumer.subscribe([TOPIC])
        
        logger.info(f"Consommation du topic {TOPIC} en cours…")
        
        events = []
        max_messages = 500
        timeout = 5.0  # s'arrêter après 5s d'inactivité
        
        start_time = datetime.now()
        while len(events) < max_messages:
            msg = consumer.poll(1.0)
            if msg is None:
                if (datetime.now() - start_time).total_seconds() > timeout:
                    break
                continue
                
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    logger.error(f"Erreur Kafka : {msg.error()}")
                    break
                    
            try:
                event_data = json.loads(msg.value().decode("utf-8"))
                events.append(event_data)
            except Exception as e:
                logger.error(f"Erreur de décodage JSON : {e}")
                
        consumer.close()
        
        logger.info(f"Consommé {len(events)} événements tardifs.")
        
        if not events:
            return []
            
        # Connexion Postgres pour insertion
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cur = conn.cursor()
        
        inserted_count = 0
        affected_dates = set()
        
        # Récupérer tous les track_ids pour validation de clés étrangères
        cur.execute("SELECT id FROM tracks")
        valid_tracks = set(str(r[0]) for r in cur.fetchall())
        
        try:
            for event in events:
                event_id = event.get("event_id")
                user_id = event.get("user_id")
                track_id = event.get("track_id")
                source_peer = event.get("source_peer")
                timestamp_str = event.get("timestamp")
                duration_ms = event.get("duration_ms", 0)
                device_type = event.get("device_type")
                geo_country = event.get("geo_country")
                completed = event.get("completed", False)
                event_source = event.get("event_source", "p2p")
                
                if not event_id or not user_id or not track_id or not timestamp_str:
                    logger.warning(f"Événement malformé skippé : {event_id}")
                    continue
                    
                if track_id not in valid_tracks:
                    logger.warning(f"Track ID inconnu ({track_id}) skippé pour l'évenement {event_id}")
                    continue
                
                try:
                    # Conversion timestamp
                    # Retrait du 'Z' pour postgres
                    clean_ts = timestamp_str.replace("Z", "")
                    ts_val = datetime.fromisoformat(clean_ts)
                    
                    cur.execute("""
                        INSERT INTO listening_events 
                            (id, user_id, track_id, source_peer_id, timestamp, duration_ms, device_type, geo_country, completed, event_source)
                        VALUES (%s, %s, %s, CAST(%s AS UUID), %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (id) DO NOTHING
                    """, (event_id, user_id, track_id, source_peer, ts_val, duration_ms, device_type, geo_country, completed, event_source))
                    
                    inserted_count += 1
                    affected_dates.add(ts_val.date().strftime("%Y-%m-%d"))
                    
                except Exception as ex:
                    logger.warning(f"Erreur d'insertion dans Postgres pour event {event_id} : {ex}")
                    
            conn.commit()
            logger.info(f"✅ {inserted_count} événements insérés dans listening_events.")
            return list(affected_dates)
            
        finally:
            cur.close()
            conn.close()

    @task(task_id="recalculate_aggregates")
    def recalculate_aggregates(affected_dates: list):
        if not affected_dates:
            logging.info("Aucune date affectée, pas de recalcul.")
            return
            
        logging.info(f"Recalcul des agrégats pour les dates : {affected_dates}")
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        
        for date_str in affected_dates:
            logging.info(f"Recalcul de daily_streams pour le : {date_str}")
            
            recalc_query = """
                INSERT INTO daily_streams (track_id, date, total_streams, unique_listeners, total_duration_ms, countries, updated_at)
                SELECT
                    track_id,
                    DATE(timestamp) AS date,
                    COUNT(*) AS total_streams,
                    COUNT(DISTINCT user_id) AS unique_listeners,
                    COALESCE(SUM(duration_ms), 0) AS total_duration_ms,
                    ARRAY_AGG(DISTINCT geo_country) FILTER (WHERE geo_country IS NOT NULL) AS countries,
                    NOW() AS updated_at
                FROM listening_events
                WHERE DATE(timestamp) = %s
                  AND completed = TRUE
                GROUP BY track_id
                ON CONFLICT (track_id, date) DO UPDATE SET
                    total_streams = EXCLUDED.total_streams,
                    unique_listeners = EXCLUDED.unique_listeners,
                    total_duration_ms = EXCLUDED.total_duration_ms,
                    countries = EXCLUDED.countries,
                    updated_at = NOW();
            """
            hook.run(recalc_query, parameters=(date_str,))
            
        logging.info("✅ Recalcul complété pour toutes les dates affectées.")

    # Orchestration
    recalculate_aggregates(consume_and_insert_late_events())
