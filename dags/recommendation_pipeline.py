"""
DAG : recommendation_pipeline
================================
Génère les recommandations personnalisées via collaborative filtering
et les stocke dans Redis + PostgreSQL.

Dépend de aggregation_pipeline via ExternalTaskSensor.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.sensors.external_task import ExternalTaskSensor

DAG_DOC = """
## recommendation_pipeline
### Rôle
Génère un top-10 de recommandations par utilisateur actif
via collaborative filtering (similarité cosinus entre profils d'écoute).

### Dépendances
Attend la fin de `aggregation_pipeline` via ExternalTaskSensor.

### Destinations
- Redis : clé `reco:{user_id}` → liste de track_ids (TTL 24h)
- PostgreSQL : table `recommendations`

### Algorithme
Collaborative filtering simplifié :
1. Construire la matrice user × track (écoutes des 7 derniers jours)
2. Calculer la similarité cosinus entre utilisateurs
3. Pour chaque user, recommander les tracks aimés par ses voisins
   mais qu'il n'a pas encore écoutés
"""

DEFAULT_ARGS = {
    "owner":             "spotify-team",
    "depends_on_past":   False,
    "start_date":        datetime(2025, 1, 1),
    "retries":           1,
    "retry_delay":       timedelta(minutes=10),
    "execution_timeout": timedelta(minutes=45),
}

POSTGRES_CONN_ID = "spotify_postgres"
REDIS_URL        = "redis://redis:6379/1"
RECO_TTL_SECONDS = 86400   # 24 heures
TOP_N_RECO       = 10
LOOKBACK_DAYS    = 7


with DAG(
    dag_id="recommendation_pipeline",
    default_args=DEFAULT_ARGS,
    description="Collaborative filtering → recommandations Redis + PostgreSQL",
    schedule_interval="0 5 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["spotify", "phase-1", "recommendation", "ml"],
    doc_md=DAG_DOC,
) as dag:

    wait_for_aggregation = ExternalTaskSensor(
        task_id="wait_for_aggregation",
        external_dag_id="aggregation_pipeline",
        external_task_id=None,
        allowed_states=["success"],
        timeout=3600,
        poke_interval=60,
        mode="reschedule",
    )

    @task(task_id="build_user_track_matrix")
    def build_user_track_matrix(**context) -> dict:
        """
        Construit la matrice user x track des écoutes des 7 derniers jours.
        Ne garde que les utilisateurs avec >= 3 écoutes distinctes.
        """
        import logging
        import pandas as pd
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        logger = logging.getLogger(__name__)

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cur  = conn.cursor()

        # Récupérer les écoutes des 7 derniers jours
        cur.execute("""
            SELECT user_id, track_id, COUNT(*) as play_count
            FROM listening_events
            WHERE timestamp >= NOW() - INTERVAL '7 days'
              AND completed = TRUE
            GROUP BY user_id, track_id
        """)

        rows = cur.fetchall()
        cur.close()
        conn.close()

        if not rows:
            logger.info("Aucune écoute trouvée sur les 7 derniers jours")
            return {"matrix": {}, "active_users": [], "all_tracks": []}

        # Construire un DataFrame
        df = pd.DataFrame(rows, columns=["user_id", "track_id", "play_count"])
        df["user_id"]  = df["user_id"].astype(str)
        df["track_id"] = df["track_id"].astype(str)

        # Garder uniquement les users avec >= 3 écoutes distinctes
        user_counts = df.groupby("user_id")["track_id"].nunique()
        active_users = user_counts[user_counts >= 3].index.tolist()
        df = df[df["user_id"].isin(active_users)]

        if df.empty:
            logger.info("Pas assez d'utilisateurs actifs pour générer des recommandations")
            return {"matrix": {}, "active_users": [], "all_tracks": []}

        # Construire la matrice {user_id: {track_id: play_count}}
        matrix = {}
        for _, row in df.iterrows():
            user  = row["user_id"]
            track = row["track_id"]
            count = int(row["play_count"])
            if user not in matrix:
                matrix[user] = {}
            matrix[user][track] = count

        all_tracks = df["track_id"].unique().tolist()

        logger.info(f"Matrice construite — {len(active_users)} users actifs, {len(all_tracks)} tracks")

        return {
            "matrix":       matrix,
            "active_users": active_users,
            "all_tracks":   all_tracks,
        }

    @task(task_id="compute_recommendations")
    def compute_recommendations(matrix_data: dict, **context) -> dict:
        """
        Calcule les recommandations par similarité cosinus.
        Pour chaque user, recommande les tracks aimés par ses voisins
        mais qu'il n'a pas encore écoutés.
        """
        import logging
        import numpy as np
        from sklearn.metrics.pairwise import cosine_similarity

        logger = logging.getLogger(__name__)

        matrix      = matrix_data.get("matrix", {})
        active_users = matrix_data.get("active_users", [])
        all_tracks  = matrix_data.get("all_tracks", [])

        if not matrix or len(active_users) < 2:
            logger.info("Pas assez de données pour calculer les recommandations")
            return {}

        # Construire la matrice numpy user x track
        track_index = {track: i for i, track in enumerate(all_tracks)}
        user_index  = {user: i for i, user in enumerate(active_users)}

        n_users  = len(active_users)
        n_tracks = len(all_tracks)

        mat = np.zeros((n_users, n_tracks))
        for user, tracks in matrix.items():
            if user in user_index:
                u_idx = user_index[user]
                for track, count in tracks.items():
                    if track in track_index:
                        t_idx = track_index[track]
                        mat[u_idx, t_idx] = count

        # Calculer la similarité cosinus entre tous les utilisateurs
        sim_matrix = cosine_similarity(mat)

        recommendations = {}

        for user in active_users:
            u_idx = user_index[user]

            # Trouver les 5 voisins les plus similaires (excluant lui-même)
            sim_scores = list(enumerate(sim_matrix[u_idx]))
            sim_scores = sorted(sim_scores, key=lambda x: x[1], reverse=True)
            sim_scores = [(i, s) for i, s in sim_scores if i != u_idx and s > 0][:5]

            if not sim_scores:
                continue

            # Tracks déjà écoutées par l'utilisateur
            already_listened = set(matrix.get(user, {}).keys())

            # Agréger les tracks des voisins pondérées par similarité
            track_scores = {}
            for neighbor_idx, similarity in sim_scores:
                neighbor = active_users[neighbor_idx]
                for track, count in matrix.get(neighbor, {}).items():
                    if track not in already_listened:
                        if track not in track_scores:
                            track_scores[track] = 0.0
                        track_scores[track] += similarity * count

            # Top N recommandations
            top_tracks = sorted(track_scores.items(), key=lambda x: x[1], reverse=True)
            recommendations[user] = [t for t, _ in top_tracks[:TOP_N_RECO]]

        logger.info(f"Recommandations générées pour {len(recommendations)} utilisateurs")
        return recommendations

    @task(task_id="store_recommendations")
    def store_recommendations(recommendations: dict, **context) -> dict:
        """
        Stocke les recommandations dans Redis (TTL 24h) et PostgreSQL.
        """
        import json
        import logging
        import redis
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        logger = logging.getLogger(__name__)

        if not recommendations:
            logger.info("Aucune recommandation à stocker")
            return {"users_with_recos": 0, "total_recommendations": 0}

        # ── Redis ─────────────────────────────────────────────
        r = redis.from_url(REDIS_URL, decode_responses=True)
        redis_count = 0

        for user_id, track_ids in recommendations.items():
            try:
                r.setex(
                    f"reco:{user_id}",
                    RECO_TTL_SECONDS,
                    json.dumps(track_ids),
                )
                redis_count += 1
            except Exception as e:
                logger.warning(f"Erreur Redis pour user {user_id} : {e}")

        logger.info(f"✅ Redis — {redis_count} utilisateurs mis à jour")

        # ── PostgreSQL ────────────────────────────────────────
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cur  = conn.cursor()

        total_recos = 0
        now = datetime.utcnow()

        for user_id, track_ids in recommendations.items():
            for rank, track_id in enumerate(track_ids):
                score = 1.0 - (rank / TOP_N_RECO)  # score décroissant
                try:
                    cur.execute("""
                        INSERT INTO recommendations
                            (user_id, track_id, score, generated_at)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (user_id, track_id)
                        DO UPDATE SET
                            score        = EXCLUDED.score,
                            generated_at = EXCLUDED.generated_at
                    """, (user_id, track_id, score, now))
                    total_recos += 1
                except Exception as e:
                    logger.warning(f"Erreur PostgreSQL reco {user_id}/{track_id} : {e}")

        conn.commit()
        cur.close()
        conn.close()

        logger.info(f"✅ PostgreSQL — {total_recos} recommandations stockées")

        return {
            "users_with_recos":    len(recommendations),
            "total_recommendations": total_recos,
        }

    # ── Orchestration ─────────────────────────────────────────
    matrix          = build_user_track_matrix()
    recommendations = compute_recommendations(matrix)
    stats           = store_recommendations(recommendations)

    wait_for_aggregation >> matrix