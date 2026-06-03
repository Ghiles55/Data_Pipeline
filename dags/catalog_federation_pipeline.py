"""
DAG : catalog_federation_pipeline
==================================
Publie les morceaux du catalogue local sur le topic partagé `catalog_federation`
et consomme les morceaux des autres groupes en validant leur structure.
"""

from datetime import datetime, timedelta
import json
import logging
import os

from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

DAG_DOC = """
## catalog_federation_pipeline

### Rôle
1. **Publication** : Récupère les morceaux du catalogue local et les envoie sur le topic Kafka partagé `catalog_federation`.
2. **Consommation** : Lit le même topic, ignore ses propres messages, valide les messages des autres groupes avec le schéma de contrat, et écrit dans la table PostgreSQL `federated_catalog`.
3. **DLQ** : Envoie les messages incorrects dans `dead_letter_events`.
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
TOPIC            = "catalog_federation"
GROUP_NAME       = "groupe-a"
CONTRACT_PATH    = "/opt/airflow/contracts/catalog_federation_schema.json"

with DAG(
    dag_id="catalog_federation_pipeline",
    default_args=DEFAULT_ARGS,
    description="Fédération inter-groupes des catalogues musicaux",
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["spotify", "phase-3", "federation", "kafka"],
    doc_md=DAG_DOC,
) as dag:

    @task(task_id="publish_local_catalog")
    def publish_local_catalog():
        logger = logging.getLogger(__name__)
        from confluent_kafka import Producer
        
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cur = conn.cursor()
        
        try:
            # Récupérer les tracks locales
            cur.execute("""
                SELECT 
                    t.id::text AS track_id,
                    a.name AS artist_name,
                    t.title AS track_title,
                    t.duration_ms,
                    t.genre,
                    COALESCE(t.audio_file_path, 'http://localhost:8080') AS audio_peer_endpoint
                FROM tracks t
                JOIN artists a ON t.artist_id = a.id
            """)
            rows = cur.fetchall()
            
            logger.info(f"Trouvé {len(rows)} morceaux locaux à fédérer.")
            
            # Initialiser le producteur Kafka
            producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
            
            sent_count = 0
            for r in rows:
                track_id, artist_name, track_title, duration_ms, genre, endpoint = r
                payload = {
                    "track_id":            track_id,
                    "source_group":        GROUP_NAME,
                    "artist_name":         artist_name,
                    "track_title":         track_title,
                    "duration_ms":         duration_ms,
                    "genre":               genre,
                    "audio_peer_endpoint": endpoint
                }
                
                producer.produce(
                    topic=TOPIC,
                    key=track_id,
                    value=json.dumps(payload).encode("utf-8")
                )
                sent_count += 1
                
            producer.flush(10)
            logger.info(f"✅ {sent_count} morceaux locaux envoyés sur le topic {TOPIC}.")
            
        finally:
            cur.close()
            conn.close()

    @task(task_id="consume_and_federate_catalogs")
    def consume_and_federate_catalogs():
        logger = logging.getLogger(__name__)
        from confluent_kafka import Consumer, KafkaError
        from jsonschema import validate, ValidationError
        
        # Charger le schéma de contrat
        try:
            with open(CONTRACT_PATH, "r", encoding="utf-8") as f:
                schema = json.load(f)
        except Exception as err:
            # Fallback : chercher le contrat relativement au dossier du DAG
            dag_dir = os.path.dirname(os.path.abspath(__file__))
            alt_path = os.path.join(dag_dir, "..", "contracts", "catalog_federation_schema.json")
            if os.path.exists(alt_path):
                with open(alt_path, "r", encoding="utf-8") as f:
                    schema = json.load(f)
            else:
                schema = None
                logger.error(f"Impossible de lire le contrat de données : {err}")
        
        # Initialiser le consommateur
        conf = {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "group.id":          "catalog_federation_airflow_consumer",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False
        }
        consumer = Consumer(conf)
        consumer.subscribe([TOPIC])
        
        events = []
        max_messages = 300
        timeout = 5.0
        
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
                payload = json.loads(msg.value().decode("utf-8"))
                events.append(payload)
            except Exception as e:
                logger.error(f"JSON invalide sur {TOPIC} : {e}")
                
        consumer.close()
        
        logger.info(f"Consommé {len(events)} messages du topic de fédération.")
        
        if not events:
            return
            
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cur = conn.cursor()
        
        valid_inserts = []
        invalid_count = 0
        
        try:
            for item in events:
                src_group = item.get("source_group")
                
                # Ignorer ses propres messages
                if src_group == GROUP_NAME:
                    continue
                    
                # Validation du contrat
                is_valid = True
                error_msg = ""
                if schema:
                    try:
                        validate(instance=item, schema=schema)
                    except ValidationError as ve:
                        is_valid = False
                        error_msg = str(ve.message)
                else:
                    # Simple validation de présence si schéma introuvable
                    if not item.get("track_id") or not item.get("artist_name") or not item.get("track_title"):
                        is_valid = False
                        error_msg = "Champs obligatoires manquants"
                
                if is_valid:
                    valid_inserts.append((
                        item["track_id"],
                        item["source_group"],
                        item.get("artist_name"),
                        item.get("track_title"),
                        item.get("duration_ms"),
                        item.get("genre"),
                        item.get("audio_peer_endpoint")
                    ))
                else:
                    # Insertion DLQ
                    invalid_count += 1
                    cur.execute("""
                        INSERT INTO dead_letter_events (original_topic, payload, error_type, error_message, status)
                        VALUES (%s, %s, %s, %s, 'pending')
                    """, (TOPIC, json.dumps(item), "invalid_contract", error_msg))
            
            # Insérer les catalogues valides
            if valid_inserts:
                cur.executemany("""
                    INSERT INTO federated_catalog 
                        (track_id, source_group, artist_name, track_title, duration_ms, genre, audio_peer_endpoint)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (track_id, source_group) DO UPDATE SET
                        artist_name = EXCLUDED.artist_name,
                        track_title = EXCLUDED.track_title,
                        duration_ms = EXCLUDED.duration_ms,
                        genre = EXCLUDED.genre,
                        audio_peer_endpoint = EXCLUDED.audio_peer_endpoint,
                        ingested_at = NOW()
                """, valid_inserts)
                
            conn.commit()
            logger.info(f"✅ Fédération terminée : {len(valid_inserts)} insérés, {invalid_count} en DLQ.")
            
        finally:
            cur.close()
            conn.close()

    publish_local_catalog() >> consume_and_federate_catalogs()
