# Tutoriel pratique Tuto-Dagster (Streamify-Ops)

Ce tutoriel vous guide dans vos premières interactions avec l'interface Dagster.

---

## 1. Installation de l'environnement

```bash
uv venv
uv pip install -e ".[dev]"
```

---

## 2. Lancement du serveur Dagster

```bash
uv run dagster dev -m streamify_ops
```

Ouvrez ensuite l'interface web dans votre navigateur sur [http://localhost:3000](http://localhost:3000).

---

## 3. Tour de l'interface (UI)

1. Rendez-vous dans la section **Assets**.
2. Ouvrez la vue **Global Asset Lineage**.
3. Observez le graphe de dépendances : `raw_tracks_data` $\rightarrow$ `clean_tracks` $\rightarrow$ `daily_artist_stats`.
4. Cliquez sur `clean_tracks` pour constater la présence de l'Asset Check de qualité `clean_tracks_id_not_null`.

---

## 4. Action pratique : Rattrapage de données (Backfill 30 jours)

1. Cliquez sur l'asset `raw_tracks_data` (ou sur le bouton général en haut à droite).
2. Cliquez sur **Materialize**, puis sélectionnez **Backfill**.
3. Choisissez les 30 derniers jours dans le sélecteur de partitions.
4. Lancez l'exécution du backfill et observez la progression en direct dans l'onglet **Runs**.

---

## 5. Notes clés sur les partitions et Software-Defined Assets

1. Définition des partitions avec `DailyPartitionsDefinition(start_date="2024-01-01")`.
2. Récupération de la date de la tranche traitée avec `context.partition_key`.
3. Cette date sert à paramétrer la source de données, tracer le lignage et organiser le stockage partitionné dans `data/`.
