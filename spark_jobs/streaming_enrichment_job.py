import os
import argparse
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
CHECKPOINT_PATH  = "s3a://spotify-checkpoints/streaming_enrichment"
PARQUET_OUT_PATH = "s3a://spotify-parquet/enriched"
POSTGRES_URL     = os.getenv("SPOTIFY_POSTGRES_URL", "jdbc:postgresql://postgres:5432/spotify")
POSTGRES_PROPS   = {
    "user":     "spotify",
    "password": "spotify",
    "driver":   "org.postgresql.Driver",
}

# ─────────────────────────────────────────────────────────────
# SCHEMAS
# ─────────────────────────────────────────────────────────────

LISTENING_EVENT_SCHEMA = StructType([
    StructField("event_id",     StringType(),    False),
    StructField("user_id",      StringType(),    False),
    StructField("track_id",     StringType(),    False),
    StructField("source_peer",  StringType(),    True),
    StructField("timestamp",    StringType(),    False),
    StructField("duration_ms",  IntegerType(),   True),
    StructField("device_type",  StringType(),    True),
    StructField("geo_country",  StringType(),    True),
    StructField("completed",    BooleanType(),   True),
    StructField("event_source", StringType(),    True),
])

P2P_NETWORK_EVENT_SCHEMA = StructType([
    StructField("event_id",      StringType(),    False),
    StructField("event_type",    StringType(),    False),
    StructField("peer_id",       StringType(),    False),
    StructField("timestamp",     StringType(),    False),
    StructField("track_id",      StringType(),    True),
    StructField("chunk_size_kb", IntegerType(),   True),
    StructField("target_peer",   StringType(),    True),
    StructField("latency_ms",    IntegerType(),   True),
    StructField("geo_country",   StringType(),    True),
    StructField("device_type",   StringType(),    True),
])

# ─────────────────────────────────────────────────────────────
# INITIALISATION SPARK
# ─────────────────────────────────────────────────────────────

def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("SPOTIFY-streaming-enrichment")
        .config("spark.sql.shuffle.partitions", "6")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .config("spark.hadoop.fs.s3a.endpoint",             "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key",           "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key",           "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access",    "true")
        .config("spark.hadoop.fs.s3a.impl",                 "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )

def main():
    parser = argparse.ArgumentParser(description="SPOTIFY streaming_enrichment_job")
    parser.add_argument("--trigger", choices=["processing", "once"], default="processing")
    args, _ = parser.parse_known_args()

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print("Démarrage du job d'enrichissement streaming…")

    # 1. Lecture des flux Kafka
    listening_raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", "listening_events")
        .option("kafka.isolation.level", "read_committed")
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    p2p_raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", "p2p_network_events")
        .option("kafka.isolation.level", "read_committed")
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # Parser les JSONs et extraire event_time avec watermarks
    listening_parsed = (
        listening_raw
        .selectExpr("CAST(value AS STRING) AS json_str")
        .select(F.from_json(F.col("json_str"), LISTENING_EVENT_SCHEMA).alias("data"))
        .select("data.*")
        .withColumn("event_time", F.to_timestamp(F.regexp_replace(F.col("timestamp"), "Z$", "")))
        .withWatermark("event_time", "2 minutes")  # Watermark de 2 min pour la jointure stream-stream
    )

    p2p_parsed = (
        p2p_raw
        .selectExpr("CAST(value AS STRING) AS json_str")
        .select(F.from_json(F.col("json_str"), P2P_NETWORK_EVENT_SCHEMA).alias("data"))
        .select("data.*")
        .withColumn("net_event_time", F.to_timestamp(F.regexp_replace(F.col("timestamp"), "Z$", "")))
        .withWatermark("net_event_time", "2 minutes")
    )

    # 2. Lecture du catalogue statique PostgreSQL
    tracks_static = (
        spark.read
        .format("jdbc")
        .option("url", POSTGRES_URL)
        .option("dbtable", "tracks")
        .option("user", POSTGRES_PROPS["user"])
        .option("password", POSTGRES_PROPS["password"])
        .option("driver", POSTGRES_PROPS["driver"])
        .load()
        .select(
            F.col("id").alias("static_track_id"),
            F.col("title").alias("track_title"),
            F.col("artist_id").alias("static_artist_id"),
            F.col("genre").alias("track_genre")
        )
    )

    artists_static = (
        spark.read
        .format("jdbc")
        .option("url", POSTGRES_URL)
        .option("dbtable", "artists")
        .option("user", POSTGRES_PROPS["user"])
        .option("password", POSTGRES_PROPS["password"])
        .option("driver", POSTGRES_PROPS["driver"])
        .load()
        .select(
            F.col("id").alias("static_artist_id"),
            F.col("name").alias("artist_name")
        )
    )

    # Joindre tracks et artists statiques pour avoir le catalogue enrichi complet
    catalog_static = tracks_static.join(
        artists_static,
        "static_artist_id",
        "inner"
    )

    # 3. Jointure Stream-Static (écoutes x catalogue)
    # On renomme ou filtre les clés pour éviter les ambiguïtés
    stream_catalog_joined = listening_parsed.join(
        catalog_static,
        listening_parsed.track_id == catalog_static.static_track_id,
        "inner"
    )

    # 4. Jointure Stream-Stream (écoutes x p2p_network_events)
    # Join on track_id and window constraint (watermark 2 minutes)
    stream_stream_joined = stream_catalog_joined.join(
        p2p_parsed,
        (stream_catalog_joined.track_id == p2p_parsed.track_id) &
        (stream_catalog_joined.event_time >= p2p_parsed.net_event_time - F.expr("INTERVAL 2 MINUTES")) &
        (stream_catalog_joined.event_time <= p2p_parsed.net_event_time + F.expr("INTERVAL 2 MINUTES")),
        "inner"
    )

    # 5. Déduplication par event_id
    deduplicated_df = stream_stream_joined.dropDuplicates(["event_id"])

    # Projection des colonnes finales
    final_df = deduplicated_df.select(
        stream_catalog_joined.event_id,
        stream_catalog_joined.user_id,
        stream_catalog_joined.track_id,
        stream_catalog_joined.track_title,
        stream_catalog_joined.artist_name,
        stream_catalog_joined.track_genre.alias("genre"),
        stream_catalog_joined.duration_ms,
        stream_catalog_joined.device_type,
        stream_catalog_joined.geo_country,
        stream_catalog_joined.completed,
        stream_catalog_joined.event_source,
        stream_catalog_joined.event_time,
        p2p_parsed.event_type.alias("p2p_network_event_type"),
        F.date_format("event_time", "yyyy-MM-dd").alias("date"),
        F.date_format("event_time", "HH").alias("hour")
    )

    # 6. Écriture topic Kafka enriched_events
    kafka_query = (
        final_df
        .selectExpr("CAST(event_id AS STRING) AS key", "to_json(struct(*)) AS value")
        .writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("topic", "enriched_events")
        .option("checkpointLocation", CHECKPOINT_PATH + "/kafka")
        .start()
    )

    # 7. Écriture MinIO au format Parquet, partitionné par date/hour
    parquet_query = (
        final_df
        .writeStream
        .format("parquet")
        .option("checkpointLocation", CHECKPOINT_PATH + "/parquet")
        .partitionBy("date", "hour")
        .start(PARQUET_OUT_PATH)
    )

    print("Écriture en cours (Kafka enriched_events + MinIO Parquet)…")
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()
