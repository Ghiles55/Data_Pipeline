"""
DAG : reconciliation_pipeline
==================================
Compare les agrégats de la Batch Layer (daily_streams) et de la Speed Layer (realtime_top_tracks).
Détecte les divergences de volume pour chaque morceau et stocke le rapport.
Alerte si la divergence dépasse 5%.
"""

from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

DAG_DOC = """
## reconciliation_pipeline

### Rôle
Vérifie la convergence de l'architecture Lambda.
Compare les streams cumulés quotidiens calculés par le batch et le streaming.

### Algorithme
1. Crée la table `reconciliation_reports` si elle n'existe pas.
2. Pour la date d'exécution (par défaut hier) :
   - Agrège `realtime_top_tracks` sur la journée.
   - Récupère les données de `daily_streams`.
   - Calcule le taux de divergence : `|batch - streaming| / batch`.
   - Alerte si divergence > 5%.
   - Insère les statistiques dans `reconciliation_reports`.
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

with DAG(
    dag_id="reconciliation_pipeline",
    default_args=DEFAULT_ARGS,
    description="Réconciliation Batch/Streaming (Architecture Lambda)",
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["spotify", "phase-3", "lambda", "reconciliation"],
    doc_md=DAG_DOC,
) as dag:

    @task(task_id="init_reconciliation_table")
    def init_reconciliation_table():
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        hook.run("""
            CREATE TABLE IF NOT EXISTS reconciliation_reports (
                id              SERIAL PRIMARY KEY,
                reconciliation_date DATE NOT NULL,
                track_id        UUID NOT NULL REFERENCES tracks(id),
                batch_count     BIGINT NOT NULL,
                streaming_count BIGINT NOT NULL,
                divergence_rate FLOAT NOT NULL,
                alert_triggered BOOLEAN DEFAULT FALSE,
                checked_at      TIMESTAMP DEFAULT NOW(),
                UNIQUE(reconciliation_date, track_id)
            );
        """)
        logging.info("✅ Table reconciliation_reports initialisée avec succès.")

    @task(task_id="run_reconciliation")
    def run_reconciliation(**context):
        logger = logging.getLogger(__name__)
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        
        # Date à reconcilier (par défaut la veille de la date d'exécution logique)
        execution_date = context.get("ds")
        reco_date = datetime.strptime(execution_date, "%Y-%m-%d").date() - timedelta(days=1)
        
        logger.info(f"Début de la réconciliation pour la date : {reco_date}")
        
        # 1. Requête pour récupérer les données consolidées batch et streaming
        # Nous faisons un FULL OUTER JOIN pour identifier les tracks manquants d'un côté ou de l'autre.
        reco_query = """
            WITH batch_data AS (
                SELECT track_id, total_streams AS batch_count
                FROM daily_streams
                WHERE date = %s
            ),
            stream_data AS (
                SELECT track_id, SUM(stream_count) AS streaming_count
                FROM realtime_top_tracks
                WHERE CAST(window_start AS DATE) = %s
                GROUP BY track_id
            )
            SELECT 
                COALESCE(b.track_id, s.track_id) AS track_id,
                COALESCE(b.batch_count, 0) AS batch_count,
                COALESCE(s.streaming_count, 0) AS streaming_count
            FROM batch_data b
            FULL OUTER JOIN stream_data s ON b.track_id = s.track_id
        """
        
        conn = hook.get_conn()
        cur = conn.cursor()
        
        try:
            cur.execute(reco_query, (reco_date, reco_date))
            rows = cur.fetchall()
            
            logger.info(f"Trouvé {len(rows)} tracks à réconcilier.")
            
            reco_inserts = []
            alerts = []
            
            for row in rows:
                track_id, b_cnt, s_cnt = row
                batch_count = float(b_cnt) if b_cnt is not None else 0.0
                streaming_count = float(s_cnt) if s_cnt is not None else 0.0
                
                # Calcul de la divergence
                max_val = max(batch_count, streaming_count)
                if max_val == 0.0:
                    divergence = 0.0
                else:
                    divergence = abs(batch_count - streaming_count) / max_val
                
                alert_triggered = divergence > 0.05
                
                reco_inserts.append((
                    reco_date,
                    track_id,
                    batch_count,
                    streaming_count,
                    divergence,
                    alert_triggered
                ))
                
                if alert_triggered:
                    alerts.append(f"⚠️ Track {track_id} : divergence = {divergence:.2%} (Batch: {batch_count}, Streaming: {streaming_count})")
            
            # 2. Insérer les résultats du rapport
            if reco_inserts:
                insert_query = """
                    INSERT INTO reconciliation_reports 
                        (reconciliation_date, track_id, batch_count, streaming_count, divergence_rate, alert_triggered)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (reconciliation_date, track_id) 
                    DO UPDATE SET
                        batch_count = EXCLUDED.batch_count,
                        streaming_count = EXCLUDED.streaming_count,
                        divergence_rate = EXCLUDED.divergence_rate,
                        alert_triggered = EXCLUDED.alert_triggered,
                        checked_at = NOW();
                """
                cur.executemany(insert_query, reco_inserts)
                conn.commit()
                logger.info(f"✅ {len(reco_inserts)} rapports insérés/mis à jour en base de données.")
                
            # 3. Log des alertes
            if alerts:
                logger.warning(f"🚨 {len(alerts)} ALERTE(S) DE DIVERGENCE BATCH/STREAMING DEPLOYEE(S) (>5%) :")
                for alert in alerts:
                    logger.warning(alert)
            else:
                logger.info("🎉 Réconciliation parfaite ! Aucune divergence supérieure à 5% détectée.")
                
            return {
                "reconciled_tracks": len(reco_inserts),
                "alerts_triggered": len(alerts)
            }
            
        finally:
            cur.close()
            conn.close()

    # Dépendance
    init_reconciliation_table() >> run_reconciliation()
