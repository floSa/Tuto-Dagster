# Tuto-Dagster (Streamify-Ops)

Projet d'apprentissage pratique de l'orchestrateur de données **Dagster**, illustré à travers un pipeline de données musicales partitionné (*Streamify-Ops*).

---

## 1. Description du projet

Ce projet a pour objectif d'aborder concrètement la philosophie *data-centric* de Dagster reposant sur les **Software-Defined Assets (SDA)**, en contraste avec l'approche historique *task-centric* (ex: Apache Airflow) centrée uniquement sur les dépendances entre tâches.

### Fonctionnalités principales :
- **Architecture médaillon (Bronze $\rightarrow$ Silver $\rightarrow$ Gold) :**
  - **Bronze (`raw_tracks_data`) :** Ingestion et simulation de données brutes via une API musicale mockée (Faker).
  - **Silver (`clean_tracks`) :** Normalisation des formats, filtrage des durées négatives et exécution d'un **Asset Check** bloquant validant l'intégrité de l'identifiant de piste.
  - **Gold (`daily_artist_stats`) :** Agrégation analytique des écoutes/titres par artiste avec intégration de prévisualisation dans les métadonnées Dagster.
- **Gestion des partitions journalières :** Définition temporelle (`DailyPartitionsDefinition`) démarrant au `2024-01-01` avec gestion du rattrapage (*backfill*).
- **Séparation calcul & stockage (IO Manager) :** Gestionnaire personnalisé `LocalFilesystemIOManager` qui sauvegarde et charge automatiquement les DataFrames au format Parquet dans le répertoire local `data/`.
- **Automatisation & Planification :** Définition d'un job partitionné et d'un schedule quotidien avec gestion du décalage (exécution à minuit pour $J-1$).

---

## 2. Arborescence du projet

```text
Tuto-Dagster/
├── data/                               # Dossier local de stockage des fichiers Parquet
│   └── .gitkeep
├── streamify_ops/                      # Module principal du pipeline Dagster
│   ├── __init__.py                     # Définitions Dagster (Definitions) et instanciation de l'IO Manager
│   ├── assets/
│   │   ├── __init__.py
│   │   ├── ingestion.py                # Asset Bronze (raw_tracks_data) et partitions quotidiennes
│   │   ├── transformation.py           # Asset Silver (clean_tracks) et Asset Check bloquant
│   │   └── analytics.py                # Asset Gold (daily_artist_stats)
│   ├── jobs.py                         # Définition du job matérialisant tous les assets partitionnés
│   ├── resources/
│   │   ├── __init__.py
│   │   └── local_filesystem_io_manager.py  # IO Manager personnalisé pour stockage Parquet
│   └── schedules.py                    # Planification quotidienne du pipeline
├── tests/
│   └── test_assets.py                  # Tests unitaires des assets en mémoire
├── main.py                             # Script d'accueil et d'information
├── pyproject.toml                      # Spécification des dépendances et métadonnées du projet (uv)
├── TUTORIAL.md                         # Guide pas-à-pas pour les manipulations dans l'interface web
└── README.md                           # Documentation principale du projet
```

---

## 3. Prérequis & Installation

### Prérequis
- Python 3.10 ou supérieur
- Gestionnaire de paquets **`uv`** installé

### Initialisation de l'environnement avec `uv`

```bash
# Se placer dans le répertoire du projet
cd ~/mes_projets/apprentissage/Tuto-Dagster

# Créer l'environnement virtuel avec uv
uv venv

# Installer les dépendances du projet ainsi que les outils de développement (tests, black)
uv pip install -e ".[dev]"
```

---

## 4. Démarrage rapide (Quickstart) & Exemples d'usage

### Lancer l'interface utilisateur web Dagster
Pour démarrer le serveur de développement et accéder à l'UI Dagster :

```bash
uv run dagster dev -m streamify_ops
```

L'interface web Dagster est alors accessible sur : **http://localhost:3000**

### Explorer et matérialiser les assets
1. Rendez-vous dans l'onglet **Assets** pour visualiser le graphe de lignage global :
   `raw_tracks_data` $\rightarrow$ `clean_tracks` $\rightarrow$ `daily_artist_stats`.
2. Cliquez sur **Materialize** pour exécuter le pipeline sur une partition donnée (ex: `2024-01-15`).
3. Pour réaliser un rattrapage de données (*backfill*) sur 30 jours, consultez le guide pas-à-pas détaillé dans [`TUTORIAL.md`](file:///W:/home/florian/mes_projets/apprentissage/Tuto-Dagster/TUTORIAL.md).

### Exécuter les tests unitaires
Les tests matérialisent les assets directement en mémoire sans dépendance externe :

```bash
uv run pytest
```

### Formater le code avec Black

```bash
uv run black .
```

---

## 5. Fonctions & Composants utiles

| Composant | Fichier source | Rôle / Responsabilité |
|---|---|---|
| `raw_tracks_data` | `streamify_ops/assets/ingestion.py` | Asset Bronze générant les écoutes brutes journalières via Faker. |
| `clean_tracks` | `streamify_ops/assets/transformation.py` | Asset Silver normalisant les données et filtrant les valeurs aberrantes. |
| `clean_tracks_id_not_null` | `streamify_ops/assets/transformation.py` | Asset Check bloquant garantissant l'absence de valeurs nulles sur `track_id`. |
| `daily_artist_stats` | `streamify_ops/assets/analytics.py` | Asset Gold agrégeant le nombre d'écoutes par artiste. |
| `LocalFilesystemIOManager` | `streamify_ops/resources/local_filesystem_io_manager.py` | IO Manager sérialisant les DataFrames en fichiers Parquet partitionnés. |
| `daily_streamify_job` | `streamify_ops/jobs.py` | Regroupement des assets dans un job exécutable. |
| `daily_streamify_schedule` | `streamify_ops/schedules.py` | Planification quotidienne (cron `0 0 * * *`) pour la partition de la veille. |
