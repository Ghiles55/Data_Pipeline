# Guide Méthodologique : Architecture, Logique et Technologies du Pipeline Spotify

Bienvenue dans ce guide ! Si vous débutez dans le monde de l'ingénierie des données (*Data Engineering*), ce document est conçu pour vous. Nous allons explorer pas à pas la logique de notre plateforme de streaming Spotify et décortiquer les technologies utilisées en les comparant à des concepts de la vie quotidienne.

---

## 1. La Vision Globale : Qu'est-ce qu'une Architecture Lambda ?

Dans un système moderne, nous devons traiter deux types de besoins :
1. **L'Exactitude Historique (Batch Layer)** : Calculer des statistiques parfaites sur le passé (ex: *"Combien de fois cette chanson a-t-elle été écoutée hier ?"*).
2. **La Réactivité Instantanée (Speed Layer)** : Savoir ce qui se passe immédiatement (ex: *"Quelles sont les chansons les plus écoutées en ce moment même ?"*).

Pour concilier ces deux besoins, nous utilisons une **Architecture Lambda**.

```
                           ┌──► Kafka ──► Spark Streaming ──► PostgreSQL (Live)  [Speed Layer]
                           │
[Simulateur d'écoutes] ────┤
                           │
                           └──► Redis ──► Airflow DAGs ─────► MinIO / PostgreSQL  [Batch Layer]
```

### L'analogie du Restaurant 🍳
* **Le Batch Layer (Airflow)** est comme le **comptable** du restaurant. En fin de journée, il prend toutes les factures papier, les vérifie une par une, calcule les impôts et ferme la caisse de manière ultra-précise. Cela prend du temps, mais c'est infaillible.
* **Le Speed Layer (Spark)** est comme le **tableau d'affichage de la cuisine**. Le chef a besoin de savoir en temps réel combien de commandes de burgers sont en attente pour s'organiser *immédiatement*. Il n'a pas besoin d'une précision comptable au centime près, il a besoin de réactivité.

---

## 2. L'Orchestration des Données avec Apache Airflow

### Qu'est-ce qu'un orchestrateur ?
Imaginons que vous ayez 5 scripts Python à lancer dans un ordre précis : le script B a besoin du résultat du script A, et les scripts C et D peuvent tourner en même temps. 
Vous pourriez écrire un gros script qui fait tout, mais s'il plante au milieu, comment relancer uniquement ce qui a échoué ?
C'est le rôle d'**Apache Airflow** : c'est le **chef d'orchestre** de vos données.

### Les concepts clés d'Airflow
* **DAG (Directed Acyclic Graph)** : C'est le plan de route. Un ensemble de tâches reliées par des flèches, sans boucle fermée (acyclique).
* **Tâche (Task)** : Une étape élémentaire du plan (ex: télécharger un fichier, lancer une requête SQL).
* **XCom (Cross-Communication)** : Un petit système de messagerie interne permettant aux tâches de s'échanger de petites informations (comme des compteurs ou des chemins de fichiers).

### Exemple concret de code (TaskFlow API)
Voici comment nous écrivons un DAG moderne en Python :

```python
from airflow.decorators import dag, task

@dag(schedule_interval="@daily", start_date=datetime(2025, 1, 1))
def mon_pipeline():
    
    @task
    def extraire_donnees():
        return {"valeur": 42}
        
    @task
    def transformer_donnees(data):
        # On multiplie par 2
        return data["valeur"] * 2

    # Dépendance logique : extraire tourne d'abord, puis transformer consomme son résultat
    transformer_donnees(extraire_donnees())
```

---

## 3. Stockage : Base de Données (PostgreSQL) vs Stockage d'Objets (MinIO)

Notre architecture utilise deux manières de stocker l'information, car elles répondent à des besoins différents.

| Caractéristique | Base de Données Relationnelle (PostgreSQL) 🗄️ | Stockage d'Objets (MinIO / S3) 📦 |
|---|---|---|
| **Type de données** | Structurées (lignes, colonnes, types stricts) | Semi-structurées ou brutes (fichiers JSON, Parquet, Images) |
| **Cas d'usage** | Requêtes rapides, relations (jointures), transactions critiques | Archivage de gros volumes, fichiers bruts historiques |
| **Analogie** | Un **classeur de fiches papier** bien triées et indexées. | Un **grand hangar** avec des boîtes étiquetées. |

### Pourquoi utiliser le format Parquet sur MinIO ?
Au lieu de stocker nos écoutes en JSON ou en CSV, nous les stockons en format **Parquet**.
* **JSON/CSV** écrivent les données ligne par ligne :
  `Nom, Genre, Pays`
  `Morceau1, Pop, FR`
* **Parquet** écrit les données **colonne par colonne** et les compresse fortement :
  `Genres: [Pop, Pop, Rock, Pop]`
  `Pays: [FR, US, FR, ES]`

**L'intérêt ?** Si vous voulez calculer le genre le plus écouté, un outil d'analyse n'a besoin de lire *que* la colonne "Genre". Il ignore complètement le reste du fichier. Le traitement est 10 à 100 fois plus rapide et prend beaucoup moins de place sur le disque.

---

## 4. Stratégie de Transformation : ETL vs ELT

Ces deux acronymes décrivent l'ordre dans lequel on extrait (**E**xtract), transforme (**T**ransform) et charge (**L**oad) la donnée.

```
ETL :  Source ──► [ Transformation en mémoire ] ──► Destination Propre
ELT :  Source ──► Destination Brute ──► [ Transformation SQL dans la destination ]
```

### L'analogie de la Cuisine 🍅
* **ETL (Extract - Transform - Load)** : Vous achetez des légumes au marché. Vous les lavez, les épluchez et les coupez **avant** de les mettre dans votre frigo. Votre frigo ne contient que des ingrédients prêts à cuire.
  * *Dans notre projet* : Le DAG [catalog_ingestion_pipeline](file:///Users/ghilesmekdam/Projets/Data_Pipeline/dags/catalog_ingestion_pipeline.py) extrait les JSON de MinIO, supprime les doublons en mémoire, valide les champs, puis insère les données propres dans PostgreSQL.
* **ELT (Extract - Load - Transform)** : Vous achetez des légumes et vous les mettez directement en vrac dans votre frigo. C'est uniquement au moment de préparer la recette que vous sortez les légumes du frigo pour les éplucher et les couper.
  * *Dans notre projet* : Le DAG [aggregation_pipeline](file:///Users/ghilesmekdam/Projets/Data_Pipeline/dags/aggregation_pipeline.py) utilise l'ELT. Les données d'écoutes brutes sont déjà dans PostgreSQL. Nous lançons de grosses requêtes SQL (`INSERT INTO ... SELECT ... GROUP BY ...`) pour transformer et agréger les données directement au sein de la base.

---

## 5. Messagerie Temps Réel : Redis vs Apache Kafka

Pour transporter les événements d'écoute générés par nos utilisateurs, nous utilisons deux technologies de messagerie.

### L'analogie des Médias 📻
* **Redis Pub/Sub** est comme une **station de radio**. Le simulateur émet et si personne n'écoute au même moment, le message est perdu à jamais. C'est ultra-rapide mais volatil.
* **Apache Kafka** est comme un **service de streaming vidéo (Netflix)**. Les messages sont écrits dans des "topics" et stockés sur le disque. Si votre consommateur (le script de lecture) tombe en panne pendant une heure, il peut reprendre exactement là où il s'est arrêté sans perdre aucune donnée.

### Pourquoi Kafka est indispensable en production ?
1. **La Rejouabilité** : On peut relire les messages reçus il y a 3 jours en cas de bug.
2. **Le Partitionnement (Scalabilité)** : Un topic Kafka peut être divisé en plusieurs **partitions** (par exemple, 6 partitions pour notre topic `listening_events`). Plusieurs machines peuvent ainsi lire différentes partitions en parallèle sans se marcher sur les pieds.
3. **L'Idempotence** : Grâce à `enable.idempotence=True`, Kafka garantit qu'un message envoyé deux fois en raison d'une micro-coupure réseau ne sera écrit qu'une seule fois.

---

## 6. Calcul Distribué en Continu avec Apache Spark

### Pourquoi Spark ?
Si vous avez 10 événements par seconde, un simple script Python suffit. Mais si vous avez **100 000 événements par seconde** provenant de téléphones du monde entier, votre machine va saturer.
**Apache Spark** résout ce problème en distribuant le calcul sur un cluster de machines (un Master qui distribue le travail, et des Workers qui l'exécutent).

### Concepts du streaming temps réel
Pour analyser les flux, Spark Structured Streaming utilise des concepts de fenêtres temporelles :

* **Tumbling Window (Fenêtre Fixe)** : Fenêtres contiguës qui ne se chevauchent pas (ex: de 12h00 à 12h05, puis de 12h05 à 12h10).
  * *Exemple Spotify* : Calculer le top 10 des morceaux écoutés toutes les 5 minutes.
* **Sliding Window (Fenêtre Glissante)** : Fenêtres qui se chevauchent (ex: une fenêtre de 15 minutes, recalculée toutes les 5 minutes).
  * *Exemple Spotify* : Déterminer la tendance des genres musicaux sur le dernier quart d'heure avec une mise à jour fréquente.

```
Tumbling (5m) : [  12h00 - 12h05  ][  12h05 - 12h10  ][  12h10 - 12h15  ]
Sliding (15m/5m):
  Fenêtre 1   : [  12h00 --------------------- 12h15  ]
  Fenêtre 2   :        [  12h05 --------------------- 12h20  ]
  Fenêtre 3   :              [  12h10 --------------------- 12h25  ]
```

---

## 7. Gestion des Erreurs : La Dead Letter Queue (DLQ)

Dans un pipeline de données, la règle d'or est : **ne jamais perdre de données de valeur**.
Si un utilisateur envoie un événement d'écoute mal formé (par exemple, un identifiant de chanson vide ou une date corrompue), nous ne devons pas :
* Faire planter tout le pipeline (interruption de service).
* Ignorer silencieusement l'événement (perte d'informations cliniques pour la facturation des labels).

### La solution : La quarantaine d'événements
Nous isolons ces messages dans une table PostgreSQL appelée `dead_letter_events` (notre **Dead Letter Queue**).
Un DAG dédié, le [dlq_reprocessing_pipeline](file:///Users/ghilesmekdam/Projets/Data_Pipeline/dags/dlq_reprocessing_pipeline.py), tourne toutes les heures pour analyser cette quarantaine :
* Si l'erreur est corrigeable (ex: date manquante réassignée avec la date de réception), le message est réparé et réinjecté dans le circuit normal.
* Si le message échoue 3 fois de suite, il est marqué comme `abandoned` pour qu'un ingénieur l'analyse manuellement.
