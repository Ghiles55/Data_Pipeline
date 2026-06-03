"""
DAG : global_top50_pipeline
==================================
Calcule le Top 50 Global en agrégeant les streams partagés par tous les groupes
sur le topic Kafka `global_metrics`, et stocke le résultat final dans Redis (clé `top50:global`).
"""

from datetime import datetime, timedelta
import json
import logging

from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

DAG_DOC = """
## global_top50_pipeline

### Rôle
1. **Publication** : Récupère les agrégats de streams locaux depuis Postgres (`daily_streams` ou `realtime_top_tracks`)
   et les publie sur le topic Kafka partagé `global_metrics`.
2. **Consommation** : Consomme les métriques publiées par les autres groupes sur le même topic.
3. **Calcul & Synchro** : Combine les écoutes de toutes les instances, trie par volume,
   et écrit le Top 50 Global dans Redis sous la clé `top50:global`.
"""

DEFAULT_ARGS = {
    "owner":             "spotify-team",
    "depends_on_past":   False,
    "start_date":        datetime(2025, 1, 1),
    "retries":           1,
    "retry_delay":       timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=15),
}

POSTGRES_CONN_ID = "spotify_postgres"
KAFKA_BOOTSTRAP  = "kafka-1:9092,kafka-2:9094,kafka-3:9096"
TOPIC            = "global_metrics"
GROUP_NAME       = "groupe-a"

with DAG(
    dag_id="global_top50_pipeline",
    default_args=DEFAULT_ARGS,
    description="Top 50 Global SPOTIFY (Agrégation cross-groupes)",
    schedule_interval="@hourly",
    catchup=False,
    max_active_runs=1,
    tags=["spotify", "phase-3", "cross-group", "redis"],
    doc_md=DAG_DOC,
) as dag:

    @task(task_id="publish_local_metrics")
    def publish_local_metrics():
        logger = logging.getLogger(__name__)
        from confluent_kafka import Producer
        
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cur = conn.cursor()
        
        try:
            # Récupérer les top tracks locaux sur les dernières 24h
            cur.execute("""
                SELECT track_id::text, SUM(stream_count) as total_streams
                FROM realtime_top_tracks
                WHERE window_start >= NOW() - INTERVAL '24 HOURS'
                GROUP BY track_id
                ORDER BY total_streams DESC
                LIMIT 50
            """)
            rows = cur.fetchall()
            
            # Si pas d'events récents, utiliser daily_streams
            if not rows:
                logger.info("Pas d'événements temps réel récents, bascule sur daily_streams…")
                cur.execute("""
                    SELECT track_id::text, total_streams
                    FROM daily_streams
                    ORDER BY total_streams DESC
                    LIMIT 50
                """)
                rows = cur.fetchall()
                
            top_tracks = [{"track_id": r[0], "stream_count": int(r[1])} for r in rows]
            
            payload = {
                "group_id":   GROUP_NAME,
                "timestamp":  datetime.utcnow().isoformat() + "Z",
                "top_tracks": top_tracks
            }
            
            producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
            producer.produce(
                topic=TOPIC,
                key=GROUP_NAME,
                value=json.dumps(payload).encode("utf-8")
            )
            producer.flush(10)
            logger.info(f"✅ Métriques du catalogue local publiées sur {TOPIC}.")
            
            return top_tracks
            
        finally:
            cur.close()
            conn.close()

    @task(task_id="compute_and_save_global_top50")
    def compute_and_save_global_top50(local_top_tracks: list):
        logger = logging.getLogger(__name__)
        from confluent_kafka import Consumer, KafkaError
        import redis
        
        # 1. Consommer les métriques des autres groupes
        conf = {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "group.id":          "global_metrics_airflow_consumer",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False
        }
        consumer = Consumer(conf)
        consumer.subscribe([TOPIC])
        
        metrics = []
        max_messages = 100
        timeout = 5.0
        
        start_time = datetime.now()
        while len(metrics) < max_messages:
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
                metrics.append(payload)
            except Exception as e:
                logger.error(f"JSON invalide sur {TOPIC} : {e}")
                
        consumer.close()
        
        logger.info(f"Consommé {len(metrics)} messages de métriques globales.")
        
        # 2. fusionner les tops tracks par track_id
        # Dictionnaire global track_id -> stream_count
        global_map = {}
        
        # Ajouter le top local
        for t in local_top_tracks:
            global_map[t["track_id"]] = t["stream_count"]
            
        # Ajouter les tops des autres groupes
        for group_metric in metrics:
            # Ignorer ses propres métriques déjà insérées
            if group_metric.get("group_id") == GROUP_NAME:
                continue
                
            tracks_list = group_metric.get("top_tracks", [])
            for track_item in tracks_list:
                tid = track_item.get("track_id")
                count = track_item.get("stream_count", 0)
                if tid:
                    global_map[tid] = global_map.get(tid, 0) + count
                    
        # 3. Trier pour obtenir le top 50 final
        sorted_top = sorted(global_map.items(), key=lambda x: x[1], reverse=True)[:50]
        final_top50 = [{"track_id": tid, "stream_count": count} for tid, count in sorted_top]
        
        # 4. Stocker dans Redis sous la clé top50:global
        redis_host = "redis"
        redis_port = 6379
        redis_db   = 1
        
        r = redis.Redis(host=redis_host, port=redis_port, db=redis_db, decode_responses=True)
        r.set("top50:global", json.dumps(final_top50))
        
        logger.info(f"✅ Top 50 Global sauvegardé dans Redis (clé top50:global) avec {len(final_top50)} morceaux.")
        return final_top50

    compute_and_save_global_top50(publish_local_metrics())
