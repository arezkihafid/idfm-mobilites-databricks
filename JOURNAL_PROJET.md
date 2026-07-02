# Journal de projet — Data Product IDFM Mobilités

Historique complet de la construction du projet, des décisions techniques et des bugs surmontés,
dans l'ordre chronologique de l'échange avec Claude Code.

---

## 1. Contexte initial

**Demande du manager :**
Construire un Data Product "Île-de-France Mobilités" sur Databricks (compte personnel Azure) en partant de
zéro. Le management (manager + PO) joue un rôle de Product Owner purement fonctionnel, sans connaissance
technique de Databricks. La responsabilité de la conception technique revient entièrement à l'équipe
technique.

**Données d'entrée fournies :**
- `Données de référentiels.zip` — 6 fichiers Parquet issus de l'Open Data IDFM/PRIM
  (arrêts transporteur, arrêts de référence, zones d'arrêts, zones de correspondance, lignes, transporteurs)
- `Données de validations.zip` — 140 fichiers couvrant 10 ans (2015-2025) :
  - 124 fichiers CSV/TXT historiques (hétérogènes : séparateur, encodage, noms de colonnes)
  - 16 fichiers Parquet 2025
  - 4 jeux de données : NB_FER, NB_SURFACE, PROFIL_FER, PROFIL_SURFACE

**PDFs de documentation IDFM/PRIM lus :**
- `2023_idfm_referentiels.pdf` — schéma de la hiérarchie des arrêts (5 niveaux)
- `Donnees_de_validation.pdf` — définition des validations télébilletiques, catégories de titres, garde-fous métier

---

## 2. Étape 1 — Atelier de cadrage

### Décision
Avant toute implémentation, préparer les questions pour l'atelier de cadrage avec le manager et le PO.

### Livrable
**`Atelier_cadrage_questions_IDFM.xlsx`** — 2 onglets :
- **"Questions atelier de cadrage"** : 27 questions structurées en thèmes (Données, Niveaux d'agrégation,
  Granularité temporelle, KPI, Gouvernance, Qualité), avec colonnes Thème / Question / Pourquoi / Réponse / Statut.
  Deux questions marquées "Bloquant" :
  - *Schéma drift* : constat de 10 ans d'hétérogénéité de colonnes dans les CSV/TXT.
  - *ID_ZDC ambigu* : le même identifiant désigne-t-il un Arrêt de référence ou une Zone De Correspondance
    (deux niveaux distincts dans la hiérarchie) ? Non résolu à ce stade.
- **"Architecture Databricks v2"** : documentation de l'architecture cible avec Unity Catalog,
  couches Bronze / Silver / Gold, et orchestration.

**Bug rencontré :** `openpyxl StyleProxy unhashable` lors de la copie de couleur de cellule.
Fix : extraire `row[0].fill.start_color.rgb` en string et créer un nouveau `PatternFill(...)`.

---

## 3. Étape 2 — Architecture

### Décision : architecture médaillon sur Unity Catalog

```
Catalog : idfm_mobilites
├── bronze          (raw append-only, tout en string pour CSV)
│   ├── arrets_transporteur, arrets_reference, zones_arrets
│   ├── zones_correspondance, lignes, transporteurs
│   ├── frequentation_ferre_raw, frequentation_surface_raw
│   ├── profil_ferre_raw, profil_surface_raw
│   └── Volume : landing/
│       ├── referentiel/
│       └── frequentation/
│           ├── historique/   (CSV/TXT 2015-2024)
│           └── recent/       (Parquet 2025)
├── silver          (schéma stable, typage, déduplication SCD Type 1)
│   ├── arret_transporteur, arret_reference, zone_arrets
│   ├── zone_correspondance, ligne, transporteur
│   ├── frequentation_ferre, frequentation_surface
│   └── profil_horaire_ferre, profil_horaire_surface
├── silver_config   (initialement prévu pour table de mapping, abandonné — voir §5.2)
├── gold            (modèle en étoile)
│   ├── dim_arret_ou_zone, dim_ligne, dim_temps
│   ├── dim_transporteur, dim_titre_transport
│   ├── fait_validation_jour, fait_profil_horaire
│   └── vues KPI : kpi_validations_mensuelles_par_mode,
│                  kpi_top_arrets_ferres, kpi_profil_horaire_moyen
└── sandbox         (expérimentations ad hoc)
```

**Livrable :** `docs/architecture.png` — diagramme généré en Python/matplotlib (13×14 pouces, 160 DPI),
montrant le flux complet Bronze → Silver → Gold avec légende colorée par couche.

**Bug rencontré :** `matplotlib` non installé localement.
Fix : `python -m pip install matplotlib --quiet`.

---

## 4. Étape 3 — Implémentation des notebooks Databricks

### 4.1 `00_setup/00_create_catalog_schemas.py`
- Crée le catalog `idfm_mobilites`, tous les schemas, le volume `bronze/landing` et ses 4 sous-répertoires.
- Fonctions clés : `spark.sql("CREATE CATALOG IF NOT EXISTS ...")`, `dbutils.fs.mkdirs(...)`.

### 4.2 `01_bronze/01_bronze_referentiel.py`
- Ingère les 6 fichiers Parquet du référentiel en mode **append**.
- Ajoute les colonnes techniques `_source_file` et `_ingested_at`.

### 4.3 `01_bronze/02_bronze_frequentation.py` — le plus complexe

**Problème principal : 10 ans de schema drift.**
Constat après inspection de tous les fichiers : nommage instable (MAJUSCULES vs minuscules, tirets vs
underscores), séparateurs différents (`;` en 2015S1, tabulation ensuite), encodages différents
(UTF-16 BOM sur 2015-2016, UTF-8 après).

**Fonctions clés :**

| Fonction | Rôle |
|---|---|
| `classify_dataset(filename)` | NB_FER / NB_SURFACE / PROFIL_FER / PROFIL_SURFACE par mots-clés |
| `extract_period(filename)` | label période (ex: `2024T3`) via deux regex selon convention |
| `detect_encoding(path)` | lit les 4 premiers octets pour détecter BOM UTF-16 |
| `detect_delimiter(path, encoding)` | compare count(`;`) vs count(`\t`) sur la 1ère ligne |
| `lowercase_columns(df)` | uniformise les en-têtes (MAJUSCULES → minuscules) |
| `cast_all_to_string(df)` | tous les Parquet 2025 castés en string avant `mergeSchema` |
| `add_technical_columns(df, ...)` | ajoute `_source_file`, `_period_label`, `_ingested_at` |

**Constante :**
```python
TRIMESTRE_FR = {"1er": "T1", "2eme": "T2", "3eme": "T3", "4eme": "T4"}
```

**Bugs rencontrés et fixes :**
- **UTF-16 BOM** : fichiers 2015-2016 illisibles. Fix : `detect_encoding()` lit les 4 premiers octets.
- **JOUR/jour en double** après `mergeSchema` : Fix : `lowercase_columns()` appliqué à chaque fichier.
- **Types Parquet 2025 hétérogènes** (ex: `pourcentage_validations` double vs string) : Fix : `cast_all_to_string()`.
- **Validation sur 124 noms** : test Python local pour valider `classify_dataset` + `extract_period` sur
  tous les noms réels — 0 erreur.

### 4.4 `02_silver/01_silver_frequentation.py`

**Problème : schema drift sur 10 ans + ambiguïté ID_ZDC.**

**Décision d'architecture :** abandonner la table de config `silver_config.schema_mapping` (prévue
initialement) car les dates de transition diffèrent par dataset et sont imprécisément connues.
Remplacée par une approche **coalesce des alias connus**.

**Fonctions clés :**

| Fonction | Rôle |
|---|---|
| `coalesce_if_exists(df, *candidates)` | `F.coalesce()` uniquement sur les colonnes présentes dans df |
| `jointure_type_arret(df)` | trace l'alias d'origine : `ARRET` / `ZDC_A_CONFIRMER` / `INCONNU_2025` |
| `parse_jour(col)` | double format date : `dd/MM/yyyy` puis `dd/MM/yy` via `try_to_date` |
| `clean_nb_validations(col)` | gère "Moins de 5" (RGPD) → NULL + `is_truncated=True` |
| `normalize_categorie_titre(col)` | "AUTRES TITRES" → "AUTRE TITRE" (harmonisation) |

**Mapping colonnes par dataset :**
- NB_FER : `coalesce_if_exists("id_refa_lda", "id_zdc")` + fallback `ida` (cast float→long→string)
- PROFIL_FER : `coalesce_if_exists("id_refa_lda", "lda", "id_zdc")`
- NB_SURFACE : `coalesce_if_exists("id_groupoflines")`
- PROFIL_SURFACE : `coalesce_if_exists("id_groupofligne")` (casse différente !)
- Pourcentage : `F.regexp_replace(col, ",", ".")` pour les décimales avec virgule

**Bugs rencontrés et fixes :**
- **`NB_VALD` avec suffixe décimal** (ex: `71324.0`) : `.cast("long")` échoue.
  Fix : `.cast("double").cast("long")`.
- **Date duale** (`dd/MM/yyyy` vs `dd/MM/yy`) : Fix : `F.coalesce(try_to_date(...), try_to_date(...))`.
- **Comma decimal** (`12,34`) : Fix : `regexp_replace` avant cast double.
- **`try_cast` pour artversion** : `.cast("int")` plante sur certaines valeurs non numériques.
  Fix : `F.expr("try_cast(artversion as int)")`.
- **`AnalysisException`** si colonne absente dans `F.coalesce()` : Fix : `coalesce_if_exists()`.

**Colonne de traçabilité `id_jointure_type` :**
Problème non résolu : `ID_ZDC` dans les données peut désigner soit un Arrêt de référence (renommage)
soit une Zone De Correspondance (niveau hiérarchique différent). La colonne `id_jointure_type` trace
l'alias d'origine pour permettre l'audit post-atelier.

### 4.5 `02_silver/02_silver_referentiel.py`

**SCD Type 1** : `keep_latest_version(df, key_col, version_col, changed_col)` via
`Window.partitionBy(key_col).orderBy(version_col desc).row_number() == 1`.
Tables Silver produites : `arret_transporteur`, `arret_reference`, `zone_arrets`,
`zone_correspondance`, `ligne`, `transporteur`.

### 4.6 `03_gold/01_gold_dimensions.py`

| Dimension | Source | Particularité |
|---|---|---|
| `dim_temps` | `sequence(2015-01-01, current_date()+365)` | Calendrier 2015→aujourd'hui+1 an |
| `dim_arret_ou_zone` | `unionByName(arret_reference, zone_correspondance)` | `granularite` discriminant : ARRET / ZDC_A_CONFIRMER |
| `dim_ligne` | `silver.ligne` groupé par `id_groupe_lignes` | Grain "ligne administrative" (plusieurs lignes commerciales par groupe) |
| `dim_transporteur` | `silver.transporteur` | Copie directe |
| `dim_titre_transport` | Hardcodé (7 valeurs stables IDFM) | IMAGINE R, NAVIGO, AMETHYSTE, TST, FGT, AUTRE TITRE, NON DEFINI |

### 4.7 `03_gold/02_gold_faits_kpi.py`

**Tables de faits :**
- `fait_validation_jour` — grain : jour × point (arrêt ou ligne) × titre × mode_reseau
  - ferré : `id_point = id_arret_jointure`, `id_groupe_lignes = NULL`
  - surface : `id_point = NULL`, `id_groupe_lignes = id_ligne_jointure`
- `fait_profil_horaire` — grain : cat_jour × tranche_horaire × point × mode_reseau

Les deux tables sont **partitionnées par `mode_reseau`** (FER / SURFACE).

**3 vues KPI d'exemple :**
- `kpi_validations_mensuelles_par_mode` — SUM mensuel par mode
- `kpi_top_arrets_ferres` — TOP arrêts ferrés avec jointure `dim_arret_ou_zone`
- `kpi_profil_horaire_moyen` — AVG % par mode/cat_jour/tranche_horaire

---

## 5. Étape 4 — Déploiement

### 5.1 `deploy/deploy_to_databricks_repo.ps1`

Script PowerShell qui :
1. Vérifie que le remote Git est configuré, commit et pousse vers GitHub.
2. Appelle l'API Databricks Repos (`/api/2.0/repos`) : GET pour trouver un repo existant par path,
   POST pour créer ou PATCH pour mettre à jour la branche.

Paramètres : `-GitRemoteUrl`, `-GitProvider`, `-DatabricksHost`, `-DatabricksToken`, `-RepoPath`, `-Branch`.

**Raison du choix REST :** Databricks CLI non installé → utilisation directe de `Invoke-RestMethod`.

### 5.2 `deploy/deploy_workflows.ps1`

Script PowerShell qui :
1. Lit les 2 fichiers JSON de workflow.
2. Remplace le placeholder `<REPO_PATH>` par le paramètre.
3. GET `/api/2.1/jobs/list` → trouve par nom → POST create ou POST /reset si existant.

### 5.3 `deploy/workflow_idfm_referentiel.json`
- Schedule : `0 0 6 * * ?` (tous les jours à 6h Paris)
- 4 tâches : `setup_catalog → bronze_referentiel → silver_referentiel → gold_dimensions`

### 5.4 `deploy/workflow_idfm_frequentation.json`
- Schedule : `0 0 6 1 1,4,7,10 ?` (trimestriel : jan/avr/juil/oct à 6h)
- 3 tâches : `bronze_frequentation → silver_frequentation → gold_faits_kpi`
- `num_workers = 2` (données plus volumineuses)

---

## 6. Étape 5 — README et documentation

**`README.md`** — 7 sections :
1. Contexte et objectifs
2. Les données (schémas détaillés avec types)
3. Architecture (diagramme PNG + capture Databricks)
   - Section 3.5 : modèle en étoile Gold avec `docs/gold_tables_databricks.png`
   - Section 3.6 : procédure de mise en place des données dans la landing zone
4. Difficultés et bugs surmontés (7 bugs documentés)
5. Commandes utilisées (git, GitHub CLI, pip)
6. Structure du repo
7. État actuel et prochaines étapes

**`docs/architecture.png`** — généré avec matplotlib.
**`docs/gold_tables_databricks.png`** — capture d'écran réelle Databricks (`mobilite_test_ws`)
montrant `SHOW TABLES IN idfm_mobilites.gold;` avec les 10 tables/vues créées.

---

## 7. Étape 6 — GitHub

**Repo public :** `https://github.com/arezkihafid/idfm-mobilites-databricks`

Historique des commits :
```
3645330  Initial commit: Data Product IDFM Mobilités - notebooks Databricks, classeur de cadrage, scripts de déploiement
f869a36  Ajout du diagramme d'architecture (PNG) dans le README
d30c370  Ajout capture d'écran des tables Gold exécutées sur Databricks
1f71cf2  README : remplacement de 'Baptiste' par 'le PO' dans la description du contexte projet
```

---

## 8. Points ouverts (à traiter en atelier)

| # | Sujet | Impact | Statut |
|---|---|---|---|
| 1 | **Ambiguïté ID_ZDC** : rename de ID_REFA_LDA ou vrai ID Zone De Correspondance ? | Bloquant pour fiabiliser les KPI ferré 2023+ | Ouvert — question IDFM/PRIM |
| 2 | **SCD Type 2** nécessaire pour le référentiel ? | Impact sur la modélisation Silver | À confirmer en atelier |
| 3 | **KPI concrets** : exemples réels à obtenir du manager/PO | Permet de raffiner les vues Gold | À obtenir en atelier |
| 4 | **Fichier sans année** (`...-1er-trimestre.parquet`) | Supposé 2025 T1, ingéré avec `default_year=2025` | À confirmer avec IDFM |
| 5 | **Catégories jour IDFM** (JOHV/SAHV/JOVS/SAVS/DIJFP) | `dim_temps` approximée par jour de semaine | Définition exacte à récupérer auprès d'IDFM |

---

## 9. Commandes clés utilisées

```powershell
# Création du repo GitHub
gh repo create idfm-mobilites-databricks --public

# Push initial
git init
git remote add origin https://github.com/arezkihafid/idfm-mobilites-databricks.git
git add databricks/ deploy/ docs/ Atelier_cadrage_questions_IDFM.xlsx README.md .gitignore
git commit -m "Initial commit: ..."
git push -u origin main

# Déployer les Workflows (exemple)
.\deploy\deploy_workflows.ps1 `
  -DatabricksHost "https://<workspace>.azuredatabricks.net" `
  -DatabricksToken "<token>" `
  -RepoPath "/Repos/<user>/idfm-mobilites-databricks"
```

```python
# Installer les dépendances locales utilisées
python -m pip install openpyxl matplotlib pyarrow --quiet
```

---

*Document généré à partir de l'historique complet de l'échange de construction du projet (juin–juillet 2026).*
