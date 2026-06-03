# Runbook — Issue #12 : Migration simulateur P2P → Kafka

> Objectif : le simulateur publie **simultanément dans Redis (Phase 1) ET dans
> Kafka (Phase 2)**. Critère de validation : Kafka UI → topic `listening_events`
> → events JSON en flux continu, **et** les DAGs Phase 1 toujours verts (Redis intact).

## Ce que j'ai fait (côté code)

1. **`src/p2p_simulator/simulator.py`** — ajout du producteur Kafka :
   - `_publish_to_kafka()` implémenté ; appelé dans `_publish_event()` **à côté** de
     la publication Redis (dual-write). Redis n'est jamais touché → DAGs batch OK.
   - Producteur configuré comme demandé par l'issue :
     `acks=all` (toutes les répliques in-sync) **et** `enable.idempotence=True`
     (pas de doublons en cas de retry).
   - **Tolérant aux pannes** : si `confluent-kafka` n'est pas installé ou que le
     broker est injoignable, le simulateur loggue un warning et continue en
     **Redis-only** — la Phase 1 ne casse jamais.
   - `flush()` au shutdown (Ctrl-C) pour garantir l'envoi des derniers events.
   - Nouveau flag `--no-kafka` pour forcer le mode Redis-only.
   - `REDIS_URL` et `KAFKA_BOOTSTRAP` lisibles depuis l'environnement.

2. **`docker-compose.yml`** — ajout d'un listener **hôte** sur les 3 brokers.
   C'est **indispensable** : voir l'explication ci-dessous.

3. `confluent-kafka==2.3.0` est déjà dans `requirements.txt`.

## Pourquoi j'ai dû modifier docker-compose (listener hôte)

Le simulateur tourne **depuis ton poste** (comme en Phase 1 pour Redis), pas dans
le réseau Docker. Or Kafka ne fonctionne pas comme Redis : un broker répond au
client une liste d'« advertised listeners », et le client se reconnecte à
**l'adresse annoncée**.

Avant, les brokers n'annonçaient que `kafka-1:9092`, `kafka-2:9094`,
`kafka-3:9096` — des noms qui ne résolvent **que dans le réseau Docker**. Depuis
l'hôte, `kafka-2`/`kafka-3` sont introuvables → le producteur ne peut pas écrire
sur les partitions portées par ces brokers (et `listening_events` a 6 partitions
réparties sur les 3 brokers). Mapper le port ne suffit pas : c'est l'adresse
*annoncée* qui compte.

**Correction (motif standard Confluent) :** un second listener par broker,
`PLAINTEXT_HOST`, annoncé en `localhost:<port>` et mappé vers l'hôte :

| Broker  | Listener interne (Docker) | Listener hôte (annoncé) |
|---------|---------------------------|--------------------------|
| kafka-1 | `kafka-1:9092`            | `localhost:29092`        |
| kafka-2 | `kafka-2:9094`            | `localhost:29094`        |
| kafka-3 | `kafka-3:9096`            | `localhost:29096`        |

Le trafic interne (inter-broker, Kafka UI, `kafka-init`) reste sur les listeners
`PLAINTEXT` d'origine — **rien de l'issue #11 n'est cassé**. On ajoute juste une
porte d'entrée pour l'hôte. Le `KAFKA_BOOTSTRAP` du simulateur pointe donc sur
`localhost:29092,localhost:29094,localhost:29096`.

> Le `CLUSTER_ID` et le `QUORUM_VOTERS` ne changent pas → **pas besoin de
> `down -v`**, un simple `up -d` recrée les brokers.

---

## Étapes à exécuter (toi — Docker = ton poste)

### A. Installer la dépendance Python (sur l'hôte)

```bash
cd ~/Projets/Data_Pipeline
pip install confluent-kafka==2.3.0
# ou : pip install -r requirements.txt
```

### B. Recréer les brokers avec les nouveaux listeners

La stack Kafka de l'issue #11 doit déjà tourner. On recrée juste les 3 brokers
pour prendre en compte le listener hôte (pas de perte de données, pas de `down -v`) :

```bash
docker compose up -d kafka-1 kafka-2 kafka-3 kafka-ui kafka-init
docker compose ps | grep kafka      # les 3 brokers + ui doivent être "Up"
```

Vérifie que les nouveaux ports hôte sont bien exposés :

```bash
docker compose ps | grep -E "29092|29094|29096"
```

**Attendu :** tu vois `0.0.0.0:29092->29092`, `...:29094->29094`, `...:29096->29096`.

### C. (Sanity check) le broker est joignable depuis l'hôte

```bash
# nc doit ouvrir la connexion sans erreur (Ctrl-C pour quitter)
nc -vz localhost 29092
```

**Attendu :** `Connection to localhost port 29092 succeeded`.

### D. Lancer le simulateur (depuis l'hôte)

```bash
cd ~/Projets/Data_Pipeline
python -m src.p2p_simulator.simulator --peers 10 --rate 5
```

**Attendu dans les logs (lignes au démarrage) :**

```
[INFO] p2p_simulator — Producteur Kafka initialisé | bootstrap=localhost:29092,localhost:29094,localhost:29096
[INFO] p2p_simulator — Simulateur démarré | mode=normal | peers=10 | rate=5.0 evt/s | kafka=on
```

Le `kafka=on` confirme que le producteur est actif. Laisse tourner.

> Si tu vois `kafka=off` + un warning : soit `confluent-kafka` n'est pas installé
> (refais l'étape A), soit le broker est injoignable (refais B/C).

### E. Vérifier dans Kafka UI — **critère de validation**

Ouvre <http://localhost:8090> → cluster `spotify-local` → onglet **Topics** →
`listening_events`.

1. Onglet **Messages** : tu dois voir des events JSON **qui défilent en continu**.
   Un message ressemble à :
   ```json
   {"event_id":"…","user_id":"…","track_id":"…","source_peer":"…",
    "timestamp":"…Z","duration_ms":…,"device_type":"…","geo_country":"…",
    "completed":true,"event_source":"p2p"}
   ```
2. La colonne **Messages count** du topic augmente quand tu rafraîchis.
3. Idem sur `p2p_network_events` (le simulateur envoie ~80% listening / 20% p2p).

→ **Fais le screenshot** des messages JSON en flux dans `listening_events` : c'est
le livrable de l'issue.

#### Vérif en ligne de commande (alternative au UI)

```bash
# Consommer quelques messages depuis le début
docker compose exec kafka-1 kafka-console-consumer \
  --bootstrap-server kafka-1:9092 \
  --topic listening_events --from-beginning --max-messages 5

# Compter les messages dans le topic (somme des offsets par partition)
docker compose exec kafka-1 kafka-run-class kafka.tools.GetOffsetShell \
  --broker-list kafka-1:9092 --topic listening_events
```

**Attendu :** des lignes JSON, et des offsets > 0 répartis sur les 6 partitions.

### F. Vérifier que la Phase 1 (Redis) est toujours verte

Pendant que le simulateur tourne, les listes Redis se remplissent comme avant :

```bash
docker compose exec redis redis-cli -n 1 LLEN listening_events_list
docker compose exec redis redis-cli -n 1 LLEN p2p_network_events_list
```

**Attendu :** `LLEN` > 0 → le dual-write fonctionne, le DAG `streaming_events_pipeline`
consomme toujours ces listes. Tu peux rejouer la vérif #6 du
`RUNBOOK_VERIFICATION.md` pour confirmer un DAGRun `success`.

### G. Arrêter proprement

`Ctrl-C` dans le terminal du simulateur. Tu dois voir :

```
[INFO] p2p_simulator — Arrêt du simulateur (signal 2) — N événements publiés
[INFO] p2p_simulator — Flush Kafka en cours…
```

Le `flush` garantit que les derniers events bufferisés partent bien vers Kafka.

---

## Dépannage

| Symptôme | Cause / Action |
|----------|----------------|
| `kafka=off` au démarrage | `confluent-kafka` non installé → étape A. |
| Logs `Échec livraison Kafka` en boucle, ou timeout métadonnées | Listener hôte absent → broker pas recréé. Refais l'étape B, vérifie les ports 29092/4/6 (étape C). |
| `nc` échoue sur 29092 | Le port n'est pas mappé → `docker compose up -d kafka-1` (recrée le conteneur) puis `docker compose ps`. |
| Events dans Kafka mais Redis vide | Vérifie que Redis tourne (`docker compose ps redis`) et l'URL `redis://localhost:6379/1`. |
| Le topic `listening_events` n'existe pas | `kafka-init` n'a pas tourné → `docker compose up -d kafka-init` (cf. RUNBOOK_KAFKA.md). |
| Producteur lent / `BufferError` | Normal sous forte charge ; le code draine puis réessaie. Baisse `--rate` si besoin. |

## Récap config producteur (pour la soutenance)

- `acks=all` → un message n'est confirmé que lorsque **toutes les répliques
  in-sync** l'ont reçu (durabilité maximale, cohérent avec RF=3 / min.insync=2).
- `enable.idempotence=True` → le broker dédoublonne les retries du producteur :
  **exactly-once côté producteur**, pas de doublons même en cas de renvoi réseau.
- `compression.type=snappy` + `linger.ms=50` → meilleur débit (petits batches).
- Clé = `event_id` → répartition régulière sur les 6 partitions de `listening_events`.
