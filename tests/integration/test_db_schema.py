import pytest
from airflow.providers.postgres.hooks.postgres import PostgresHook
import os

@pytest.mark.skipif(not os.getenv("SPOTIFY_POSTGRES_CONN"), reason="Nécessite la DB locale")
def test_postgres_connection_and_schema():
    """
    Test d'intégration basique : vérifie que la connexion PostgreSQL fonctionne
    et que les tables principales existent.
    """
    hook = PostgresHook(postgres_conn_id="spotify_postgres")
    conn = hook.get_conn()
    cur = conn.cursor()
    
    # Vérifier l'existence de la table tracks
    cur.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'tracks');")
    exists = cur.fetchone()[0]
    assert exists is True, "La table 'tracks' devrait exister."
    
    cur.close()
    conn.close()
