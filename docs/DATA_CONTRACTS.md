# Contrats de Données Inter-Groupes — Spotify Data Pipeline

Ce document décrit les spécifications et les schémas d'échange de données inter-groupes pour la Phase 3. 

Toutes les données en provenance d'instances externes (autres groupes de projet) doivent être considérées comme non fiables. Elles doivent être validées selon ces contrats avant d'être insérées en base. En cas de non-conformité, les payloads défectueux sont routés vers la **Dead Letter Queue (DLQ)**.

---

## 1. Contrat de Fédération de Catalogue (`catalog_federation`)
Le topic partagé `catalog_federation` permet à chaque instance Spotify de publier ses nouveautés musicales et d'importer celles des autres groupes.

* **Schéma JSON** : [catalog_federation_schema.json](file:///Users/ghilesmekdam/Projets/Data_Pipeline/contracts/catalog_federation_schema.json)
* **Format du message** :
```json
{
  "track_id": "84863387-efd9-4812-959a-d7afbaa362c2",
  "source_group": "groupe-b",
  "artist_name": "Sunset Project",
  "track_title": "Summer Vibes",
  "duration_ms": 180000,
  "genre": "Pop",
  "audio_peer_endpoint": "http://10.0.1.5:8081/stream"
}
```

---

## 2. Contrat de Requête P2P Cross-Group (`p2p_cross_requests`)
Lorsqu'un utilisateur local cherche un morceau absent du catalogue local, le simulateur interroge les autres groupes via ce contrat.

* **Schéma JSON** : [p2p_cross_request_schema.json](file:///Users/ghilesmekdam/Projets/Data_Pipeline/contracts/p2p_cross_request_schema.json)
* **Format du message** :
```json
{
  "request_id": "00a45da1-ef23-4000-8888-ff75edd29e99",
  "requesting_group": "groupe-a",
  "target_group": "groupe-b",
  "track_id": "84863387-efd9-4812-959a-d7afbaa362c2",
  "peer_id": "34cea0cc-6a9f-4c70-afcb-61982089a531",
  "timestamp": "2026-06-03T13:00:00Z"
}
```

---

## 3. Contrat de Métriques Globales (`global_metrics`)
Ce topic sert à agréger le Top 50 Global sur l'ensemble des instances des groupes connectés.

* **Schéma JSON** : [global_metrics_schema.json](file:///Users/ghilesmekdam/Projets/Data_Pipeline/contracts/global_metrics_schema.json)
* **Format du message** :
```json
{
  "group_id": "groupe-a",
  "timestamp": "2026-06-03T14:00:00Z",
  "top_tracks": [
    {
      "track_id": "093c6ceb-7221-459f-a844-ff75edd29e99",
      "stream_count": 1450
    }
  ]
}
```

---

## 4. Politique de Sécurité et de Résilience (DLQ)
Tout message consommé depuis un topic partagé et qui échoue à la validation du schéma JSON Schema associé doit :
1. Être immédiatement intercepté (pas de crash du DAG).
2. Être inséré dans la table `dead_letter_events` locale avec l'erreur `invalid_contract`.
3. Notifier l'ingénieur via les logs de tâche.
