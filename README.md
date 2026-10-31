# Dagster Test Bench

Banc d'essai pour apprendre l'orchestrateur de données **Dagster** sur un petit pipeline musical partitionné par jour, *Streamify-Ops*. Tout tourne en local, avec des données simulées.

![Graphe des assets dans l'interface Dagster](documentation/images/14-lignage-apres-backfill.png)

## Documentation

| Objectif | À lire |
|---|---|
| Comprendre le projet et retrouver une information | [Manuel utilisateur](documentation/manuel-utilisateur.md) |
| Apprendre en manipulant : commande, écran, fichier, étape par étape | [Tutoriel](TUTORIAL.md) |

## Démarrage rapide

Prérequis : [`uv`](https://docs.astral.sh/uv/), qui installe lui-même Python 3.12.

```bash
uv sync --extra dev
```

```bash
export DAGSTER_HOME="$PWD/.dagster_home"
```

```bash
mkdir -p "$DAGSTER_HOME"
```

```bash
uv run dg dev
```

Ouvrir ensuite [http://localhost:3000](http://localhost:3000). Exporter `DAGSTER_HOME` dans chaque terminal utilisé : c'est ce qui relie les commandes à l'interface.

## Ce que le pipeline met en œuvre

- **Assets en architecture médaillon,** rangés en groupes : `raw_tracks_data` (Bronze) → `clean_tracks` (Silver) → `daily_artist_stats` (Gold).
- **Partitions journalières** depuis le 2024-01-01, avec rattrapage (*backfill*).
- **Check qualité bloquant et partitionné :** `clean_tracks_id_not_null`, et un moyen de le faire échouer à la demande.
- **IO Manager personnalisé :** écriture et lecture en Parquet dans `data/`, avec les métadonnées standard de Dagster.
- **Job et schedule quotidiens,** arrêtés par défaut, qui traitent la veille à 00:00 UTC.
- **Automatisation :** Declarative Automation, sensors de fichier et d'échec, politique de fraîcheur, retries, file d'attente des runs, dépendance entre partitions.
- **PostgreSQL :** ressource configurable, mot de passe lu dans l'environnement, export de Gold et relance après une panne de la base. Docker est nécessaire pour cette partie uniquement.
- **Structure `dg` :** tout ce qui est placé dans `src/streamify_ops/defs/` est chargé automatiquement.

## Arborescence

```text
Dagster-Test-Bench/
├── src/streamify_ops/
│   ├── definitions.py                      # Charge tout le dossier defs/
│   ├── settings.py                         # Dossier de données, mode accéléré
│   ├── defs/
│   │   ├── assets/
│   │   │   ├── ingestion.py                # Bronze : raw_tracks_data, partitions
│   │   │   ├── transformation.py           # Silver : clean_tracks, check bloquant
│   │   │   └── analytics.py                # Gold : daily_artist_stats
│   │   ├── resources.py                    # Déclare l'IO Manager
│   │   ├── jobs.py                         # daily_streamify_job
│   │   ├── schedules.py                    # daily_streamify_schedule
│   │   └── sensors.py                      # Arrivée de fichier, échec de run
│   ├── optional/
│   │   ├── weekly_artist_stats.py          # Cumul sur 7 jours, à copier dans defs/assets/
│   │   └── artist_stats_table.py           # Export PostgreSQL, à copier dans defs/assets/
│   └── resources/
│       ├── local_filesystem_io_manager.py  # Stockage Parquet
│       └── postgres.py                     # Connexion PostgreSQL
├── tests/                                  # Assets, check, schedule, sensors, automatisation
├── scripts/capture_docs.py                 # Régénère les captures de la documentation
├── documentation/
│   ├── manuel-utilisateur.md
│   └── images/
├── data/                                   # Fichiers Parquet produits (non versionnés)
├── .github/workflows/ci.yml                # black, dg check defs, pytest
├── docker-compose.yml                      # Base PostgreSQL du tutoriel
├── LICENSE
├── pyproject.toml
├── uv.lock
└── TUTORIAL.md
```

## Développement

```bash
uv run pytest
```

```bash
uv run black .
```

```bash
uv run dg check defs
```

## Licence

[MIT](LICENSE)
