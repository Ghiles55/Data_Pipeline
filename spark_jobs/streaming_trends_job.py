"""
Spark Job : streaming_trends_job
==================================
Consomme le topic Kafka `listening_events` et produit en continu
les tendances musicales temps réel.

Outputs :
    - PostgreSQL → table `realtime_top_tracks` (top 10 par fenêtre de 5 min)
    - Redis      → clé `top_tracks:live` (top genres par sliding window)

Lancement (job test #13 — affichage console) :
    spark-submit \\
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,\\
                   org.postgresql:postgresql:42.7.1,\\
                   org.apache.hadoop:hadoop-aws:3.3.4,\\
                   com.amazonaws:aws-java-sdk-bundle:1.12.262 \\
        /opt/spark-jobs/streaming_trends_job.py --mode console --trigger processing

État :
    [x] read_kafka_stream() : lecture du topic Kafka + JSON + event_time  (#13)
    [x] Sink console append + checkpoint MinIO + triggers processingTime/Once  (#13)
    [ ] Fenêtres tumbling 5 min → realtime_top_tracks (PostgreSQL)  (issue suivante)
    [ ] Sliding windows genres (15 min / 5 min) → Redis  (issue suivante)
"""

import argparse
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, BooleanType, TimestampType
)

# ─────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────

KAFKA_BOOTSTRAP  = os.getenv("KAFKA_BOOTSTRAP",  "kafka-1:9092")
KAFKA_TOPIC      = "listening_events"
CHECKPOINT_PATH  = "s3a://spotify-checkpoints/streaming_trends"
POSTGRES_URL     = os.getenv("SPOTIFY_POSTGRES_URL",
                             "jdbc:postgresql://postgres:5432/spotify")
POSTGRES_PROPS   = {
    "user":   "spotify",
    "password": "spotify",
    "driver": "org.postgresql.Driver",
}

# ─────────────────────────────────────────────────────────────
# SCHÉMA DES ÉVÉNEMENTS D'ÉCOUTE
# ─────────────────────────────────────────────────────────────

LISTENING_EVENT_SCHEMA = StructType([
    StructField("event_id",    StringType(),    False),
    StructField("user_id",     StringType(),    False),
    StructField("track_id",    StringType(),    False),
    StructField("source_peer", StringType(),    True),
    StructField("timestamp",   StringType(),    False),  # ISO 8601 → à caster en Timestamp
    StructField("duration_ms", IntegerType(),   True),
    StructField("device_type", StringType(),    True),
    StructField("geo_country", StringType(),    True),
    StructField("completed",   BooleanType(),   True),
    StructField("event_source",StringType(),    True),
])


# ─────────────────────────────────────────────────────────────
# INITIALISATION SPARK
# ─────────────────────────────────────────────────────────────

def create_spark_session() -> SparkSession:
    """
    Crée et configure la SparkSession avec les dépendances nécessaires.

    TODO : vérifier que les packages kafka et postgresql sont disponibles
    """
    return (
        SparkSession.builder
        .appName("SPOTIFY-streaming-trends")
        .config("spark.sql.shuffle.partitions", "6")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        # MinIO / S3A
        .config("spark.hadoop.fs.s3a.endpoint",             "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key",           "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key",           "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access",    "true")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )


# ─────────────────────────────────────────────────────────────
# LECTURE KAFKA
# ─────────────────────────────────────────────────────────────

def read_kafka_stream(spark: SparkSession):
    """
    Lit le topic Kafka `listening_events` en streaming et renvoie un
    DataFrame typé (une ligne = un événement d'écoute).

    Étapes :
        1. readStream.format("kafka") sur les 3 brokers
        2. value (bytes) → string
        3. from_json() avec LISTENING_EVENT_SCHEMA
        4. timestamp ISO ("…Z") → TimestampType (colonne event_time)
           pour les fenêtres temporelles des jobs suivants.

    Returns:
        DataFrame streaming avec colonnes typées + event_time
    """
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        # latest : on ne lit que les nouveaux events (flux temps réel du simulateur)
        .option("startingOffsets", "latest")
        # ne pas planter si un offset a expiré (retention.ms)
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = (
        raw
        .selectExpr("CAST(value AS STRING) AS json_str")
        .select(F.from_json(F.col("json_str"), LISTENING_EVENT_SCHEMA).alias("data"))
        .select("data.*")
        # "2026-06-03T10:39:50.436Z" → on retire le "Z" final puis on caste
        .withColumn(
            "event_time",
            F.to_timestamp(F.regexp_replace(F.col("timestamp"), "Z$", "")),
        )
    )
    return parsed


# ─────────────────────────────────────────────────────────────
# SINK CONSOLE — JOB TEST (issue #13)
# ─────────────────────────────────────────────────────────────

def write_console_stream(events_df, trigger: str = "processing"):
    """
    Sink `console` en mode append : affiche le flux d'events pour valider
    la lecture Kafka (issue #13). Checkpoint sur MinIO comme demandé.

    trigger :
        "processing" → micro-batch toutes les 10 s (processingTime)
        "once"       → un seul batch puis arrêt (Trigger.Once)
    """
    writer = (
        events_df.writeStream
        .outputMode("append")
        .format("console")
        .option("truncate", "false")
        .option("numRows", 20)
        # Checkpoint MinIO (bucket spotify-checkpoints créé par minio-init)
        .option("checkpointLocation", CHECKPOINT_PATH + "/console")
    )

    if trigger == "once":
        writer = writer.trigger(once=True)
    else:
        writer = writer.trigger(processingTime="10 seconds")

    return writer.start()


# ─────────────────────────────────────────────────────────────
# AGRÉGATIONS STREAMING
# ─────────────────────────────────────────────────────────────

def compute_top_tracks_tumbling(events_df):
    """
    Top 10 des tracks par tumbling window de 5 minutes.

    TODO :
        1. groupBy(window("event_time", "5 minutes"), "track_id")
        2. agg(count("*").alias("stream_count"), countDistinct("user_id").alias("unique_listeners"))
        3. Output mode : "update" (on met à jour au fur et à mesure)
        4. Écrire dans PostgreSQL table realtime_top_tracks

    Hint : pour écrire dans PostgreSQL depuis Spark Streaming,
    utiliser foreachBatch() et df.write.jdbc() dans le batch.
    """
    raise NotImplementedError("TODO : implémenter compute_top_tracks_tumbling()")


def compute_genre_listeners_sliding(events_df, catalog_df):
    """
    Listeners uniques par genre en sliding window (15 min glissant toutes les 5 min).

    TODO :
        1. Joindre events_df avec catalog_df (stream-static join sur track_id)
           pour récupérer le genre du morceau
        2. groupBy(window("event_time", "15 minutes", "5 minutes"), "genre")
        3. agg(countDistinct("user_id").alias("unique_listeners"))
        4. Écrire dans Redis (clé "genre_listeners:live") via foreachBatch
           Utiliser redis-py dans le batch

    Hint : charger le catalogue PostgreSQL comme DataFrame statique avec spark.read.jdbc()
    """
    raise NotImplementedError("TODO : implémenter compute_genre_listeners_sliding()")


# ─────────────────────────────────────────────────────────────
# POINT D'ENTRÉE
# ─────────────────────────────────────────────────────────────

def main():
    # parse_known_args : ignore les éventuels arguments passés par spark-submit
    parser = argparse.ArgumentParser(description="SPOTIFY streaming_trends_job")
    parser.add_argument(
        "--mode", choices=["console", "trends"], default="console",
        help="console = job test #13 (affichage) ; trends = agrégations (issues suivantes)",
    )
    parser.add_argument(
        "--trigger", choices=["processing", "once"], default="processing",
        help="processing = processingTime(10s) ; once = Trigger.Once",
    )
    args, _ = parser.parse_known_args()

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print("Démarrage streaming_trends_job...")
    print(f"Mode       : {args.mode} | trigger : {args.trigger}")
    print(f"Kafka      : {KAFKA_BOOTSTRAP} → topic : {KAFKA_TOPIC}")
    print(f"Checkpoint : {CHECKPOINT_PATH}")

    # Lecture Kafka (commune à tous les modes)
    events_df = read_kafka_stream(spark)

    if args.mode == "console":
        # ── Issue #13 : valider la lecture du topic en console ──
        query = write_console_stream(events_df, trigger=args.trigger)
        query.awaitTermination()
    else:
        # ── Agrégations temps réel (issues suivantes, seq 2.3+) ──
        # catalog_df = spark.read.jdbc(POSTGRES_URL, "tracks", properties=POSTGRES_PROPS)
        query_top_tracks = compute_top_tracks_tumbling(events_df)
        # query_genres   = compute_genre_listeners_sliding(events_df, catalog_df)
        spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
