# Runbook — Issue #11 : Cluster Kafka KRaft (3 brokers)

> ⚠️ **16 Go de RAM minimum.** Ferme les applis lourdes avant de lancer.
> Ce que j'ai fait : décommenté + corrigé le bloc Kafka dans `docker-compose.yml`.
> Ce qu'il te reste : lancer la stack, ouvrir Kafka UI, faire le screenshot (Docker = ton poste).

## Correction apportée (important)

Dans la version commentée, `kafka-1` avait un `KAFKA_CONTROLLER_QUORUM_VOTERS`
**incohérent** avec kafka-2/kafka-3 :

```
kafka-1 (avant) : 1@kafka-1:9093,2@kafka-2:9093,3@kafka-3:9093   ❌ mauvais ports
kafka-2 / kafka-3 : 1@kafka-1:9093,2@kafka-2:9095,3@kafka-3:9097  ✅
```

En KRaft, **les 3 brokers doivent avoir une chaîne quorum identique**, chaque
voter pointant vers le port CONTROLLER du broker (9093 / 9095 / 9097). Avec
l'ancienne valeur, le quorum contrôleur ne se forme pas → le cluster ne démarre
jamais. Corrigé : les 3 brokers utilisent maintenant
`1@kafka-1:9093,2@kafka-2:9095,3@kafka-3:9097`.

Autres ajustements : `KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT` explicite,
réplication/`min.insync.replicas` mis sur les 3 brokers, et `kafka-ui` / `kafka-init`
attendent les 3 brokers (et non kafka-1 seul) pour éviter un échec de création
des topics RF=3.

## 1. Démarrer le cluster

```bash
cd ~/Projets/Data_Pipeline
docker compose up -d kafka-1 kafka-2 kafka-3 kafka-ui kafka-init

# Suivre la formation du cluster (Ctrl-C pour quitter le suivi)
docker compose logs -f kafka-1
```

Attendre ~30–60 s. Les 3 brokers doivent passer en `Up`.

```bash
docker compose ps | grep kafka
```

## 2. Vérifier que le quorum contrôleur est formé

```bash
docker compose exec kafka-1 kafka-metadata-quorum \
  --bootstrap-server kafka-1:9092 describe --status
```
**Attendu :** un `LeaderId` défini et 3 `CurrentVoters` (1, 2, 3).

## 3. Vérifier les brokers et les topics

```bash
# Les 3 brokers enregistrés
docker compose exec kafka-1 kafka-broker-api-versions \
  --bootstrap-server kafka-1:9092 | grep -c "id:"

# Lister les 6 topics
docker compose exec kafka-1 kafka-topics \
  --bootstrap-server kafka-1:9092 --list
```
**Attendu (6 topics) :** `listening_events`, `p2p_network_events`,
`catalog_updates`, `enriched_events`, `fraud_alerts`, `late_listening_events`.

## 4. Vérifier les configs demandées par l'issue

```bash
# listening_events : 6 partitions, RF 3
docker compose exec kafka-1 kafka-topics \
  --bootstrap-server kafka-1:9092 --describe --topic listening_events

# catalog_updates : compaction activée
docker compose exec kafka-1 kafka-topics \
  --bootstrap-server kafka-1:9092 --describe --topic catalog_updates
```
**Attendu :**
- `listening_events` → `PartitionCount: 6`, `ReplicationFactor: 3`.
- `catalog_updates` → `Configs: cleanup.policy=compact`.

## 5. Kafka UI + screenshot (critère de validation)

Ouvrir <http://localhost:8090> → cluster `spotify-local` → onglet **Topics**.
Faire le **screenshot montrant les 6 topics** (avec partitions / RF visibles)
pour valider l'issue.

## Dépannage

| Symptôme | Action |
|----------|--------|
| Broker redémarre en boucle | `docker compose logs kafka-1` — souvent un quorum mal formé. La correction du QUORUM_VOTERS règle ce cas. |
| « cluster id doesn't match » | Volume formaté avec un ancien CLUSTER_ID : `docker compose down -v` puis relancer. |
| Brokers ne se voient pas | Vérifier qu'ils sont sur le même réseau Docker (`docker network ls`). |
| `kafka-init` échoue (replicas) | Les 3 brokers ne sont pas encore prêts ; relancer `docker compose up -d kafka-init`. |
| OOM / machine qui rame | RAM insuffisante (<16 Go) — réduire `KAFKA_HEAP_OPTS` ou fermer d'autres conteneurs. |

> Spark reste commenté (issue suivante). Ici on ne lance que les 5 services Kafka.
