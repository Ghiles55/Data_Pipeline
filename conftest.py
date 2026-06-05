"""
conftest.py — Configuration pytest pour le projet SPOTIFY

Ce fichier est automatiquement chargé par pytest.
Il configure le path Python pour que les imports src/ fonctionnent.
"""
import sys
import os

# Ajouter la racine du projet au PYTHONPATH
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Fix Airflow SQLite path issue on Windows for local testing
os.environ["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"] = "sqlite:///airflow.db"
os.environ["AIRFLOW_HOME"] = os.path.dirname(os.path.abspath(__file__))
