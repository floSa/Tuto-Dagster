# Sécurité — Dagster Test Bench

> Ce projet est un **banc d'essai local**, avec des données simulées. Ce document décrit ce qui est réellement en place et signale ce qu'il faudrait durcir avant tout usage exposé.

---

## 1. Secrets & configuration

Le seul secret du projet est le mot de passe de la base du tutoriel. Il n'est écrit ni dans [`docker-compose.yml`](../docker-compose.yml) ni dans le code applicatif : Docker Compose exige la variable, et [`artist_stats_table.py`](../src/streamify_ops/optional/artist_stats_table.py) la lit avec `EnvVar`.

| Secret | Où | Rotation |
|---|---|---|
| `STREAMIFY_PG_PASSWORD` | Variable d'environnement exportée dans chaque terminal | Manuelle : supprimer le volume, puis recréer la base |

**Vérifications réalisées :**

| Contrôle | Résultat |
|---|---|
| Page de la ressource dans l'interface | Affiche le nom de la variable, pas sa valeur |
| Mot de passe témoin recherché dans la sortie de `dg launch` | Absent |
| Mot de passe témoin recherché dans les fichiers de `$DAGSTER_HOME` et de `data/`, soit 24 fichiers | Absent |
| `dg launch` sans la variable | Échec explicite : `environment variable "STREAMIFY_PG_PASSWORD" which is not set` |
| `dg check defs` sans la variable | Réussit : la valeur n'est lue qu'à l'exécution |

Le projet ne lit pas de fichier `.env`, mais Docker Compose le ferait s'il existait : `.env` figure donc dans [`.gitignore`](../.gitignore).

> **Valeur de démonstration :** `streamify` apparaît en clair dans le [tutoriel](../TUTORIAL.md), dans [`capture_docs.py`](../scripts/capture_docs.py) et dans [`ci.yml`](../.github/workflows/ci.yml). Cette valeur ne protège qu'une base locale jetable et ne doit pas être réutilisée ailleurs.

---

## 2. Isolation réseau

| Service | Exposé à l'hôte ? | Adresse | Justification |
|---|---|---|---|
| Interface Dagster | Oui, machine locale seulement | `127.0.0.1:3000` | Valeur par défaut de `dg dev`, observée dans ses logs |
| `postgres` | Oui, machine locale seulement | `127.0.0.1:5434` | Accès depuis les runs, qui s'exécutent hors de Docker. Liaison vérifiée avec `docker port` |

Le port 5434 évite les bases déjà présentes sur les ports 5432 et 5433 de la machine de développement.

---

## 3. Dépendances

Les versions sont figées par [`uv.lock`](../uv.lock). L'audit couvre les dépendances d'exécution et de développement :

```bash
uvx pip-audit --no-deps --disable-pip -r <(uv export --frozen --all-extras --no-hashes --no-emit-project -q)
```

**Dernier résultat, le 2026-10-31 : aucune faille connue sur 113 paquets.** Le premier passage en avait signalé dans `filelock`, `certifi` et `black`, toutes corrigées par une mise à jour de `uv.lock`, après laquelle les tests sont restés au vert.

La CI n'exécute pas cet audit : il reste à relancer à la main. Elle référence ses actions par version (`actions/checkout@v4`, `astral-sh/setup-uv@v6`), pas par empreinte de commit. Playwright et Pillow, utilisés seulement par le script de captures, ne sont ni figés ni audités.

---

## 4. Conteneurs

Le projet n'utilise qu'une image officielle, `postgres:16-alpine`, sans image construite localement.

| Point | État |
|---|---|
| Version | Tag flottant sur la version majeure : 16.14 observée le 2026-10-31 |
| Durcissement | Aucun ajouté : ni `user:`, ni `read_only:`, ni `cap_drop:` |
| Sonde de santé | `pg_isready`, utilisée par `docker compose up -d --wait` |

---

## 5. Données & accès

Les données sont **entièrement simulées** par Faker : les noms d'artistes ne désignent personne de réel, et aucune donnée personnelle n'est traitée.

| Emplacement | Contenu | Versionné ? |
|---|---|---|
| `data/` | Fichiers Parquet, alertes, marqueurs de l'inbox | ❌ |
| `.dagster_home/` | Historique, logs, télémétrie | ❌ |
| Volume Docker | Table `artist_stats` | ❌ |

Un fichier d'alerte contient l'identifiant du run, la partition, le job et le message d'échec. Une erreur de connexion à la base cite l'hôte et le port, pas le mot de passe.

**Télémétrie :** Dagster crée `$DAGSTER_HOME/.telemetry/id.yaml` et `$DAGSTER_HOME/logs/event.log`. L'envoi réseau n'a pas été vérifié dans ce projet.

---

## 6. Risques connus (non traités)

- **Risque : interface sans authentification.** Toute personne ayant accès à la machine locale peut lancer des runs et modifier l'état des automatismes depuis `127.0.0.1:3000`. Acceptable pour un banc d'essai local ; un déploiement partagé exigerait un contrôle d'accès en amont.
- **Risque : connexion à la base sans chiffrement exigé.** [`PostgresResource`](../src/streamify_ops/resources/postgres.py) ne configure pas TLS. Acceptable sur la boucle locale ; à exiger dès que la base est distante.
- **Risque : compte unique.** Le rôle `streamify` possède la base, et les tests suppriment la table `artist_stats`. Un usage réel demanderait un compte aux droits limités et une base de test séparée.
- **Risque : aucune sauvegarde.** `docker compose down -v` supprime la table sans confirmation. Sans conséquence ici : le contenu se recalcule depuis Parquet.
