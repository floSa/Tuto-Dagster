# Stockage — Dagster Test Bench

Le projet range ses données à **trois endroits**, qui ne se mélangent pas. La [vue d'ensemble](architecture.md) décrit le flux qui les alimente.

---

## 1. Vue d'ensemble

| Store | Emplacement | Contenu | Écrit par | Cycle de vie |
|---|---|---|---|---|
| **Parquet** | `data/`, ou `STREAMIFY_DATA_DIR` | Données Bronze, Silver et Gold | [`LocalFilesystemIOManager`](../src/streamify_ops/resources/local_filesystem_io_manager.py) | Remplacé à chaque relance d'une date |
| **Instance Dagster** | `$DAGSTER_HOME` | Runs, événements, état des automatismes | Dagster | Conservé jusqu'à suppression |
| **PostgreSQL** | Volume Docker, port 5434 | Table `artist_stats` | [`artist_stats_table`](../src/streamify_ops/optional/artist_stats_table.py), si activé | Conservé jusqu'à `docker compose down -v` |

---

## 2. Fichiers Parquet

Un dossier par asset, un fichier par date :

```text
data/
├── raw_tracks_data/AAAA-MM-JJ.parquet
├── clean_tracks/AAAA-MM-JJ.parquet
├── daily_artist_stats/AAAA-MM-JJ.parquet
├── weekly_artist_stats/AAAA-MM-JJ.parquet   # si le cumul est activé
├── alerts/<identifiant du run>.txt          # si alert_on_run_failure est actif
└── inbox/AAAA-MM-JJ.ready                   # marqueurs déposés pour inbox_file_sensor
```

| Asset | Colonnes | Lignes |
|---|---|---|
| `raw_tracks_data` | `track_id`, `track_name`, `artist_name`, `genre`, `duration_sec`, `release_date`, `ingestion_date` | **100** |
| `clean_tracks` | Les mêmes | Environ 90 à 100 |
| `daily_artist_stats` | `artist_name`, `track_count` | **20** au plus |
| `weekly_artist_stats` | `artist_name`, `track_count` | **20** au plus |

Un fichier Bronze pèse environ **12 Ko**. Les colonnes et le nombre de lignes de chaque matérialisation sont lisibles dans **Catalog**, sans ouvrir le fichier, grâce aux métadonnées `dagster/column_schema` et `dagster/row_count`.

Le contenu de `data/` n'est pas versionné, à l'exception de `data/.gitkeep`.

**Point d'attention :** une partition Silver dont le check échoue reste écrite. Le check bloque la suite, pas l'écriture de l'asset contrôlé.

---

## 3. Instance Dagster

`$DAGSTER_HOME` contient l'historique dans des bases **SQLite** :

| Chemin | Contenu |
|---|---|
| `history/runs.db`, `history/runs/` | Runs et leurs événements |
| `schedules/schedules.db` | État et évaluations des schedules et sensors |
| `storage/` | Journaux de chaque run |
| `logs/event.log`, `.telemetry/id.yaml` | Télémétrie |
| `dagster.yaml` | Configuration de l'instance, si créée, par exemple pour la limite de runs |

Sans `DAGSTER_HOME` commun, une commande et l'interface ne partagent pas la même instance : le run réussit et le fichier est écrit, mais l'interface ne l'affiche pas.

---

## 4. Table PostgreSQL

```sql
CREATE TABLE IF NOT EXISTS artist_stats (
    partition_date DATE NOT NULL,
    artist_name TEXT NOT NULL,
    track_count INTEGER NOT NULL,
    PRIMARY KEY (partition_date, artist_name)
)
```

| Paramètre | Valeur |
|---|---|
| Hôte et port | `localhost:5434` |
| Base et utilisateur | `streamify` |
| Mot de passe | `STREAMIFY_PG_PASSWORD` |
| Création | Au premier run de l'asset |

L'asset supprime puis réinsère les lignes de la date dans **une seule transaction**. Relancer une date ne crée donc pas de doublon, et les autres dates restent intactes. Les données survivent à `docker compose stop` : les lignes écrites avant une panne simulée étaient toujours présentes après le redémarrage.

**Les tests suppriment cette table** : [`test_postgres.py`](../tests/test_postgres.py) la vide avant et après chaque test qui exige une vraie base.

---

## 5. Remettre à zéro

| Objectif | Commande | Effet |
|---|---|---|
| Vider la base | `docker compose down -v` | Supprime le conteneur et le volume |
| Vider l'historique et les fichiers | Serveur arrêté : `rm -rf .dagster_home data/*/`, puis `mkdir -p .dagster_home` | Repart de zéro dans l'interface |

Ces trois stores sont indépendants : supprimer l'un ne met pas à jour les autres. Le contenu de `data/` et de la table se recalcule à partir des dates, puisque les données simulées ne dépendent que de la date. Le projet ne prévoit ni sauvegarde ni purge automatique.
