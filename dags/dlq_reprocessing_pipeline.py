"""
DAG : dlq_reprocessing_pipeline
==================================
Retraite périodiquement les événements défectueux de la Dead Letter Queue.

Planification : toutes les heures
Catchup       : désactivé

Architecture :
    PostgreSQL dead_letter_events (status='pending')
        → fetch_pending_dlq()       ← récupérer les events à retraiter
        → reprocess_events()        ← tenter de corriger et réinjecter
        → update_dlq_status()       ← marquer reprocessed ou abandoned

TODO :
    [ ] Implémenter fetch_pending_dlq()
    [ ] Implémenter reprocess_events()
    [ ] Implémenter update_dlq_status()
    [ ] Tester avec injection de données corrompues
    [ ] Ajouter doc_md sur ce DAG
"""

from datetime import datetime, timedelta
import json
import logging

from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

DAG_DOC = """
## dlq_reprocessing_pipeline

### Rôle
Retraite les événements défectueux isolés dans `dead_letter_events`.
Tente de corriger les erreurs et de réinjecter les events valides.

### Sources
- Table `dead_letter_events` où `status = 'pending'`

### Logique de retraitement
1. Récupérer les events `pending` avec `retry_count < 3`
2. Tenter la validation et la correction
3. Si succès → réinjecter dans `listening_events` + `status = 'reprocessed'`
4. Si échec après 3 tentatives → `status = 'abandoned'`

### Test d'\''injection
```sql
INSERT INTO dead_letter_events (payload, error_type, original_topic)
VALUES ('{"user_id": null, "track_id": "invalid"}', 'missing_fields', 'listening_events');
```

### TODO
Compléter les 3 tâches marquées NotImplementedError.
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
MAX_RETRIES      = 3
BATCH_SIZE       = 100   # traiter par lots pour ne pas surcharger


with DAG(
    dag_id="dlq_reprocessing_pipeline",
    default_args=DEFAULT_ARGS,
    description="Retraitement horaire des événements Dead Letter Queue",
    schedule_interval="@hourly",
    catchup=False,
    max_active_runs=1,
    tags=["spotify", "phase-1", "dlq", "resilience"],
    doc_md=DAG_DOC,
) as dag:

    @task(task_id="fetch_pending_dlq")
    def fetch_pending_dlq(**context) -> list:
        """
        Récupère les événements en attente de retraitement.
        """
        logger = logging.getLogger(__name__)
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cur = conn.cursor()

        try:
            cur.execute("""
                SELECT id, payload, error_type, retry_count, original_topic, created_at
                FROM dead_letter_events
                WHERE status = 'pending'
                  AND retry_count < %s
                ORDER BY created_at ASC
                LIMIT %s
            """, (MAX_RETRIES, BATCH_SIZE))

            events = []
            for row in cur.fetchall():
                events.append({
                    "id": str(row[0]),
                    "payload": row[1],
                    "error_type": row[2],
                    "retry_count": row[3],
                    "original_topic": row[4],
                    "created_at": row[5],
                })

            logger.info(f"✅ {len(events)} événements pending trouvés pour retraitement")
            return events

        finally:
            cur.close()
            conn.close()

    @task(task_id="reprocess_events")
    def reprocess_events(pending_events: list, **context) -> dict:
        """
        Tente de corriger et réinjecter chaque événement défectueux.
        """
        logger = logging.getLogger(__name__)
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cur = conn.cursor()

        reprocessed = []
        failed = []

        # Récupérer tous les track_ids valides pour vérification rapide
        cur.execute("SELECT id FROM tracks")
        valid_track_ids = set(str(row[0]) for row in cur.fetchall())

        try:
            for event in pending_events:
                try:
                    # Parser le payload JSON
                    if isinstance(event["payload"], str):
                        payload = json.loads(event["payload"])
                    else:
                        payload = event["payload"]

                    # Valider/corriger les champs obligatoires
                    user_id = payload.get("user_id")
                    track_id = payload.get("track_id")
                    timestamp = payload.get("timestamp")

                    # user_id manquant → impossible à corriger → abandoned
                    if not user_id:
                        logger.warning(f"⚠️ Event {event['id']}: user_id manquant → ABANDONED")
                        failed.append({"event_id": event["id"], "reason": "missing_user_id"})
                        continue

                    # timestamp invalide → utiliser created_at comme fallback
                    if not timestamp:
                        logger.info(f"ℹ️ Event {event['id']}: timestamp manquant → utiliser created_at")
                        timestamp = event["created_at"].isoformat()
                    else:
                        # Tenter de parser le timestamp
                        try:
                            if isinstance(timestamp, str):
                                timestamp = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
                            timestamp = timestamp.isoformat() if hasattr(timestamp, "isoformat") else timestamp
                        except Exception as ts_err:
                            logger.warning(f"⚠️ Event {event['id']}: timestamp invalide → utiliser created_at")
                            timestamp = event["created_at"].isoformat()

                    # track_id inconnu → vérifier dans tracks, si absent → abandoned
                    if not track_id or str(track_id) not in valid_track_ids:
                        logger.warning(f"⚠️ Event {event['id']}: track_id '{track_id}' invalide/inconnu → ABANDONED")
                        failed.append({"event_id": event["id"], "reason": "invalid_track_id"})
                        continue

                    # Event valide → préparer pour réinsertion
                    reprocessed.append({
                        "event_id": event["id"],
                        "user_id": user_id,
                        "track_id": track_id,
                        "timestamp": timestamp,
                        "device_type": payload.get("device_type"),
                        "geo_country": payload.get("geo_country"),
                        "duration_ms": payload.get("duration_ms"),
                        "completed": payload.get("completed", False),
                        "event_source": payload.get("event_source", "p2p"),
                    })
                    logger.info(f"✅ Event {event['id']}: ready for reinsertion")

                except Exception as e:
                    logger.error(f"❌ Event {event['id']}: erreur lors du retraitement — {str(e)}")
                    failed.append({"event_id": event["id"], "reason": f"processing_error: {str(e)}"})

            logger.info(f"📊 Retraitement terminé: {len(reprocessed)} à réinjecter, {len(failed)} échoués")
            return {"reprocessed": reprocessed, "failed": failed}

        finally:
            cur.close()
            conn.close()

    @task(task_id="update_dlq_status")
    def update_dlq_status(results: dict, **context) -> dict:
        """
        Met à jour le statut des événements dans dead_letter_events.
        """
        logger = logging.getLogger(__name__)
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cur = conn.cursor()

        reprocessed_events = results.get("reprocessed", [])
        failed_events = results.get("failed", [])

        reprocessed_count = 0
        abandoned_count = 0
        pending_count = 0

        try:
            # ── Traiter les events retraités avec succès ──────────────────
            for event in reprocessed_events:
                try:
                    # INSERT dans listening_events
                    cur.execute("""
                        INSERT INTO listening_events
                            (id, user_id, track_id, timestamp, device_type,
                             geo_country, duration_ms, completed, event_source, created_at)
                        VALUES (gen_random_uuid(), %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                    """, (
                        event["user_id"],
                        event["track_id"],
                        event["timestamp"],
                        event.get("device_type"),
                        event.get("geo_country"),
                        event.get("duration_ms"),
                        event.get("completed", False),
                        event.get("event_source", "p2p"),
                    ))

                    # UPDATE dead_letter_events SET status='reprocessed'
                    cur.execute("""
                        UPDATE dead_letter_events
                        SET status = 'reprocessed', resolved_at = NOW()
                        WHERE id = %s
                    """, (event["event_id"],))

                    reprocessed_count += 1
                    logger.info(f"✅ Event {event['event_id']}: reprocessed successfully")

                except Exception as e:
                    logger.error(f"❌ Event {event['event_id']}: erreur lors de l'insertion — {str(e)}")
                    failed_events.append({"event_id": event["event_id"], "reason": f"insertion_error: {str(e)}"})

            # ── Traiter les events échoués ──────────────────────────────
            for event in failed_events:
                try:
                    event_id = event["event_id"]

                    # UPDATE dead_letter_events: incrémenter retry_count et mettre à jour le statut
                    cur.execute("""
                        UPDATE dead_letter_events
                        SET retry_count = retry_count + 1,
                            last_retry_at = NOW(),
                            status = CASE
                                WHEN retry_count + 1 >= %s THEN 'abandoned'
                                ELSE 'pending'
                            END
                        WHERE id = %s
                    """, (MAX_RETRIES, event_id))

                    # Vérifier le nouveau statut pour les logs
                    cur.execute("SELECT retry_count, status FROM dead_letter_events WHERE id = %s", (event_id,))
                    row = cur.fetchone()
                    if row:
                        new_retry_count, new_status = row[0], row[1]
                        if new_status == 'abandoned':
                            abandoned_count += 1
                            logger.warning(f"⚠️ Event {event_id}: ABANDONED after {new_retry_count} retries")
                        else:
                            pending_count += 1
                            logger.info(f"ℹ️ Event {event_id}: remains PENDING (retry {new_retry_count}/{MAX_RETRIES})")

                except Exception as e:
                    logger.error(f"❌ Event {event['event_id']}: erreur lors de la mise à jour DLQ — {str(e)}")

            # ── Commit et logging final ──────────────────────────────────
            conn.commit()

            stats = {
                "reprocessed": reprocessed_count,
                "abandoned": abandoned_count,
                "pending": pending_count,
            }

            logger.info(
                f"📊 Bilan DLQ retraitement:\n"
                f"   ✅ Retraités : {reprocessed_count}\n"
                f"   ⚠️ Abandonnés : {abandoned_count}\n"
                f"   ℹ️ Encore pending : {pending_count}"
            )

            return stats

        finally:
            cur.close()
            conn.close()

    # ── Orchestration ─────────────────────────────────────────
    pending = fetch_pending_dlq()
    results = reprocess_events(pending)
    update_dlq_status(results)
