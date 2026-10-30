# Dagster Test Bench

Banc d'essai pour apprendre l'orchestrateur de données **Dagster** sur un petit pipeline musical partitionné par jour, *Streamify-Ops*. Tout tourne en local, avec des données simulées.

![Graphe des assets dans l'interface Dagster](documentation/images/14-lignage-apres-backfill.png)

## Documentation

| Vous voulez… | Lisez |
|---|---|
| Comprendre le projet et retrouver une information | [Manuel utilisateur](documentation/manuel-utilisateur.md) |
| Apprendre en manipulant : commande, écran, fichier, étape par étape | [Tutoriel](TUTORIAL.md) |

## Démarrage rapide

Il vous faut [`uv`](https://docs.astral.sh/uv/) ; il installe lui-même Python 3.12.

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
uv run dagster dev -m streamify_ops
```

Ouvrez ensuite [http://localhost:3000](http://localhost:3000). Exportez `DAGSTER_HOME` dans chaque terminal utilisé : c'est ce qui relie vos commandes à l'interface.

## Ce que le pipeline met en œuvre

- **Assets en architecture médaillon :** `raw_tracks_data` (Bronze) → `clean_tracks` (Silver) → `daily_artist_stats` (Gold).
- **Partitions journalières** depuis le 2024-01-01, avec rattrapage (*backfill*).
- **Check qualité bloquant :** `clean_tracks_id_not_null`, et un moyen de le faire échouer à la demande.
- **IO Manager personnalisé :** écriture et lecture en Parquet dans `data/`.
- **Job et schedule quotidiens,** arrêtés par défaut, qui traitent la veille à 00:00 UTC.

## Arborescence

```text
Dagster-Test-Bench/
├── streamify_ops/
│   ├── __init__.py                         # Definitions : assets, checks, IO Manager, job, schedule
│   ├── assets/
│   │   ├── ingestion.py                    # Bronze : raw_tracks_data, partitions
│   │   ├── transformation.py               # Silver : clean_tracks, check bloquant
│   │   └── analytics.py                    # Gold : daily_artist_stats
│   ├── resources/
│   │   └── local_filesystem_io_manager.py  # Stockage Parquet
│   ├── jobs.py                             # daily_streamify_job
│   └── schedules.py                        # daily_streamify_schedule
├── tests/                                  # Tests des assets, du check et du schedule
├── scripts/capture_docs.py                 # Régénère les captures de la documentation
├── documentation/
│   ├── manuel-utilisateur.md
│   └── images/
├── data/                                   # Fichiers Parquet produits (non versionnés)
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
