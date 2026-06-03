import os
import argparse
import json
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, BooleanType, TimestampType
)

# ─────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka-1:9092")
CHECKPOINT_PATH = "s3a://spotify-checkpoints/fraud_detection"
POSTGRES_URL    = os.getenv("SPOTIFY_POSTGRES_URL", "jdbc:postgresql://postgres:5432/spotify")
POSTGRES_PROPS  = {
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
    StructField("status",        StringType(),    True),  # success/failed
])

# ─────────────────────────────────────────────────────────────
# INITIALISATION SPARK
# ─────────────────────────────────────────────────────────────

def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("SPOTIFY-fraud-detection")
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
    parser = argparse.ArgumentParser(description="SPOTIFY fraud_detection_job")
    parser.add_argument("--trigger", choices=["processing", "once"], default="processing")
    args, _ = parser.parse_known_args()

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print("Démarrage de la détection de fraudes streaming…")

    # 1. Lecture des topics Kafka
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

    # Parsing des events
    listening_parsed = (
        listening_raw
        .selectExpr("CAST(value AS STRING) AS json_str")
        .select(F.from_json(F.col("json_str"), LISTENING_EVENT_SCHEMA).alias("data"))
        .select("data.*")
        .withColumn("event_time", F.to_timestamp(F.regexp_replace(F.col("timestamp"), "Z$", "")))
    )

    p2p_parsed = (
        p2p_raw
        .selectExpr("CAST(value AS STRING) AS json_str")
        .select(F.from_json(F.col("json_str"), P2P_NETWORK_EVENT_SCHEMA).alias("data"))
        .select("data.*")
        .withColumn("net_event_time", F.to_timestamp(F.regexp_replace(F.col("timestamp"), "Z$", "")))
    )

    # 2. Règle 1 : plus de 100 écoutes en 10 min pour un même user_id (Burst listen)
    # Pour pouvoir tester en environnement local, on met une limite à > 10 écoutes en 10 min pour lever l'alerte
    rule1_df = (
        listening_parsed
        .withWatermark("event_time", "10 minutes")
        .groupBy(F.window("event_time", "10 minutes"), "user_id")
        .agg(F.count("*").alias("listen_count"))
        .filter("listen_count > 10")  # Modifié de 100 à 10 pour faciliter les tests
        .select(
            F.col("user_id"),
            F.lit(None).cast("string").alias("peer_id"),
            F.lit("burst_listen").alias("fraud_type"),
            F.min(F.lit(1.0), F.col("listen_count") / F.lit(50.0)).alias("suspicion_score"),
            F.to_json(F.struct("listen_count")).alias("evidence"),
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end")
        )
    )

    # 3. Règle 2 : durée moyenne < 5 secondes sur une fenêtre de 1 heure (Bot streaming)
    # Pour tester, on filtre sur avg_duration < 5s et count > 3
    rule2_df = (
        listening_parsed
        .withWatermark("event_time", "1 hour")
        .groupBy(F.window("event_time", "1 hour"), "user_id")
        .agg(
            F.count("*").alias("listen_count"),
            F.avg("duration_ms").alias("avg_duration_ms")
        )
        .filter("listen_count > 3 AND avg_duration_ms < 5000")
        .select(
            F.col("user_id"),
            F.lit(None).cast("string").alias("peer_id"),
            F.lit("bot_stream").alias("fraud_type"),
            F.lit(0.9).alias("suspicion_score"),
            F.to_json(F.struct("listen_count", "avg_duration_ms")).alias("evidence"),
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end")
        )
    )

    # 4. Règle 3 : taux échec transfert P2P > 50% sur 15 min (Free Rider / bad peer)
    # Pour tester, on filtre sur failed_transfers > 2
    rule3_df = (
        p2p_parsed
        .filter("event_type = 'chunk_transfer'")
        .withWatermark("net_event_time", "15 minutes")
        .groupBy(F.window("net_event_time", "15 minutes"), F.col("peer_id"))
        .agg(
            F.count("*").alias("total_transfers"),
            F.sum(F.when(F.col("status") == "failed", 1).otherwise(0)).alias("failed_transfers")
        )
        .filter("total_transfers > 2 AND (failed_transfers / total_transfers) > 0.5")
        .select(
            F.lit(None).cast("string").alias("user_id"),
            F.col("peer_id"),
            F.lit("bad_peer").alias("fraud_type"),
            (F.col("failed_transfers") / F.col("total_transfers")).alias("suspicion_score"),
            F.to_json(F.struct("total_transfers", "failed_transfers")).alias("evidence"),
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end")
        )
    )

    # Union de tous les flux d'alertes
    alerts_df = rule1_df.union(rule2_df).union(rule3_df)

    # Écriture dans Postgres et DLQ
    def write_fraud_to_postgres(batch_df, batch_id):
        if batch_df.rdd.isEmpty():
            return
        rows = batch_df.collect()
        spark_sess = batch_df.sparkSession
        jvm = spark_sess._jvm
        conn = jvm.java.sql.DriverManager.getConnection(POSTGRES_URL, POSTGRES_PROPS["user"], POSTGRES_PROPS["password"])
        try:
            ps_fraud = conn.prepareStatement("""
                INSERT INTO fraud_detections (user_id, peer_id, fraud_type, suspicion_score, evidence, window_start, window_end)
                VALUES (CAST(? AS UUID), CAST(? AS UUID), ?, ?, CAST(? AS JSONB), CAST(? AS TIMESTAMP), CAST(? AS TIMESTAMP))
            """)
            ps_dlq = conn.prepareStatement("""
                INSERT INTO dead_letter_events (original_topic, payload, error_type, error_message, status)
                VALUES (?, CAST(? AS JSONB), ?, ?, 'pending')
            """)
            for row in rows:
                user_id = row["user_id"]
                peer_id = row["peer_id"]
                fraud_type = row["fraud_type"]
                score = float(row["suspicion_score"])
                evidence_json = row["evidence"]
                win_start = row["window_start"].isoformat() if row["window_start"] else None
                win_end = row["window_end"].isoformat() if row["window_end"] else None
                
                # Insert into fraud_detections
                if user_id:
                    ps_fraud.setString(1, user_id)
                else:
                    ps_fraud.setNull(1, jvm.java.sql.Types.VARCHAR)
                    
                if peer_id:
                    ps_fraud.setString(2, peer_id)
                else:
                    ps_fraud.setNull(2, jvm.java.sql.Types.VARCHAR)
                    
                ps_fraud.setString(3, fraud_type)
                ps_fraud.setDouble(4, score)
                ps_fraud.setString(5, evidence_json)
                ps_fraud.setString(6, win_start)
                ps_fraud.setString(7, win_end)
                ps_fraud.addBatch()
                
                # Insert into dead_letter_events (DLQ)
                payload_dict = {
                    "user_id": user_id,
                    "peer_id": peer_id,
                    "fraud_type": fraud_type,
                    "suspicion_score": score,
                    "evidence": json.loads(evidence_json) if evidence_json else {},
                    "window_start": win_start,
                    "window_end": win_end
                }
                ps_dlq.setString(1, "listening_events")
                ps_dlq.setString(2, json.dumps(payload_dict))
                ps_dlq.setString(3, "fraud_alert")
                ps_dlq.setString(4, f"Suspicion de fraude detectee : {fraud_type}")
                ps_dlq.addBatch()
                
            ps_fraud.executeBatch()
            ps_dlq.executeBatch()
            ps_fraud.close()
            ps_dlq.close()
        finally:
            conn.close()

    # Query 1: Sinks Postgres (foreacBatch) + Checkpoint MinIO
    postgres_query = (
        alerts_df
        .writeStream
        .outputMode("update")
        .foreachBatch(write_fraud_to_postgres)
        .option("checkpointLocation", CHECKPOINT_PATH + "/postgres")
        .start()
    )

    # Query 2: Sinks Kafka fraud_alerts
    kafka_query = (
        alerts_df
        .selectExpr("CAST(COALESCE(user_id, peer_id) AS STRING) AS key", "to_json(struct(*)) AS value")
        .writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("topic", "fraud_alerts")
        .option("checkpointLocation", CHECKPOINT_PATH + "/kafka")
        .start()
    )

    print("Détection de fraudes active (écriture Postgres & Kafka fraud_alerts)…")
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()
