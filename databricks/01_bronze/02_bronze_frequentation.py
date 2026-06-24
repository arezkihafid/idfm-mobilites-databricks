# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze — Données de fréquentation (validations télébilletiques)
# MAGIC Ingestion brute des 4 jeux de données (NB_FER, NB_SURFACE, PROFIL_FER, PROFIL_SURFACE) sur deux familles de
# MAGIC fichiers :
# MAGIC - `frequentation/historique/` : CSV/TXT 2015-2024, séparateur et noms de colonnes hétérogènes selon la période.
# MAGIC - `frequentation/recent/` : Parquet 2025+, déjà typé mais avec des noms de colonnes propres à ce format
# MAGIC   (ex: `ida` au lieu de `ID_REFA_LDA`/`ID_ZDC`).
# MAGIC
# MAGIC **Aucune normalisation de schéma ici** : chaque fichier est chargé avec ses colonnes d'origine. La
# MAGIC réconciliation des noms de colonnes et des types se fait dans `02_silver/01_silver_frequentation.py`,
# MAGIC pilotée par la table de mapping `silver_config.schema_mapping`.
# MAGIC
# MAGIC Le classement par dataset (NB_FER / NB_SURFACE / PROFIL_FER / PROFIL_SURFACE) et par période se fait à
# MAGIC partir du nom de fichier, car ni le nommage ni la casse ne sont stables sur 10 ans (constaté en inspectant
# MAGIC les fichiers réels — cf. onglet "Architecture Databricks v2" du classeur de cadrage).

# COMMAND ----------

import re
from datetime import datetime

from pyspark.sql import functions as F, DataFrame

CATALOG = "idfm_mobilites"
LANDING_HISTO = f"/Volumes/{CATALOG}/bronze/landing/frequentation/historique"
LANDING_RECENT = f"/Volumes/{CATALOG}/bronze/landing/frequentation/recent"

TARGET_TABLES = {
    "NB_FER": f"{CATALOG}.bronze.frequentation_ferre_raw",
    "NB_SURFACE": f"{CATALOG}.bronze.frequentation_surface_raw",
    "PROFIL_FER": f"{CATALOG}.bronze.profil_ferre_raw",
    "PROFIL_SURFACE": f"{CATALOG}.bronze.profil_surface_raw",
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Classification du fichier (dataset + période) à partir de son nom
# MAGIC
# MAGIC Exemples réellement rencontrés :
# MAGIC - `2015S1_NB_FER.csv`, `2017_S2_NB_FER.txt`, `2024_T3_NB_FER.txt`
# MAGIC - `2015S2_NB_SURFACE.txt`, `2018_T1_NB_SURFACE.txt`
# MAGIC - `2023_S1_NB_FER .txt` (espace parasite avant l'extension)
# MAGIC - `validations-reseau-ferre-nombre-validations-par-jour-1er-trimestre-2025.parquet`
# MAGIC - `validations-reseau-ferre-profils-horaires-par-jour-type-1er-trimestre.parquet` (année absente du nom)
# MAGIC - `validations-sur-le-reseau-ferre-profils-horaires-par-jour-type-2eme-trimestre-2025.parquet`

# COMMAND ----------

TRIMESTRE_FR = {"1er": "T1", "2eme": "T2", "3eme": "T3", "4eme": "T4"}


def classify_dataset(filename: str) -> str:
    """Détermine NB_FER / NB_SURFACE / PROFIL_FER / PROFIL_SURFACE à partir du nom de fichier.
    Heuristique par mots-clés (et non par position) car le nommage n'est pas stable sur 10 ans."""
    name = filename.lower()
    kind = "PROFIL" if "profil" in name else "NB"
    mode = "FER" if "fer" in name else "SURFACE"
    return f"{kind}_{mode}"


def extract_period(filename: str, default_year: int = None) -> str:
    """Reconstruit un label de période (ex: '2015S1', '2024T3') à partir du nom de fichier,
    qui suit l'une des deux conventions observées dans l'historique IDFM."""
    name = filename

    # Convention "historique" : YYYY(_)S1/S2/T1..T4, ex: 2015S1, 2017_S2, 2024_T3
    m = re.search(r"(20\d{2})_?([ST][1-4])", name, re.IGNORECASE)
    if m:
        return f"{m.group(1)}{m.group(2).upper()}"

    # Convention "récente" parquet : "1er-trimestre-2025" ou "1er-trimestre" (année absente)
    m = re.search(r"(1er|2eme|3eme|4eme)-trimestre(?:-(\d{4}))?", name, re.IGNORECASE)
    if m:
        trimestre = TRIMESTRE_FR[m.group(1).lower()]
        year = m.group(2) or default_year
        if year is None:
            raise ValueError(
                f"Année absente du nom de fichier '{filename}' et aucun default_year fourni — "
                "à signaler comme anomalie (cf. atelier de cadrage)."
            )
        return f"{year}{trimestre}"

    raise ValueError(f"Impossible d'extraire la période du nom de fichier '{filename}'")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Détection du séparateur pour les fichiers CSV/TXT historiques
# MAGIC Constaté : `;` pour les tout premiers fichiers (2015S1), tabulation pour quasiment tout le reste.

# COMMAND ----------


def detect_encoding(path: str) -> str:
    """Certains fichiers historiques (2015-2016 notamment) sont encodés en UTF-16 avec BOM,
    contrairement à la majorité en UTF-8 — sans détection, la lecture plante."""
    local_path = path.replace("dbfs:", "") if path.startswith("dbfs:") else path
    with open(local_path, "rb") as f:
        raw = f.read(4)
    if raw.startswith(b"\xff\xfe") or raw.startswith(b"\xfe\xff"):
        return "UTF-16"
    return "UTF-8"

def detect_delimiter(path: str, encoding: str) -> str:
    first_line = spark.read.option("encoding", encoding).text(path).take(1)[0][0]
    return ";" if first_line.count(";") > first_line.count("\t") else "\t"


# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingestion des fichiers historiques (CSV/TXT, 2015-2024)

# COMMAND ----------

# DBTITLE 1,Cell 8

def detect_delimiter(path: str, encoding: str) -> str:
    local_path = path.replace("dbfs:", "") if path.startswith("dbfs:") else path
    with open(local_path, encoding=encoding, errors='replace') as f:
        first_line = f.readline()
    return ";" if first_line.count(";") > first_line.count("\t") else "\t"


def lowercase_columns(df: DataFrame) -> DataFrame:
    """Les en-têtes sont en MAJUSCULES dans l'historique CSV/TXT mais déjà en minuscules dans les
    parquets 2025. Sans cette uniformisation, mergeSchema créerait deux colonnes distinctes
    (ex: JOUR et jour) au lieu de les faire coïncider. Normalisation purement technique (pas de
    renommage métier ici, ça reste le rôle de Silver)."""
    return df.toDF(*[c.lower() for c in df.columns])


def add_technical_columns(df: DataFrame, source_file: str, period_label: str) -> DataFrame:
    return (
        df.withColumn("_source_file", F.lit(source_file))
        .withColumn("_period_label", F.lit(period_label))
        .withColumn("_ingested_at", F.current_timestamp())
    )


def ingest_historique_file(file_info) -> None:
    filename = file_info.name
    path = file_info.path
    dataset = classify_dataset(filename)
    period = extract_period(filename)
    encoding = detect_encoding(path)
    delimiter = detect_delimiter(path, encoding)

    df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("delimiter", delimiter)
        .option("encoding", encoding)
        .option("inferSchema", "false")  # tout en string en Bronze, le typage se fait en Silver
        .load(path)
    )
    df = lowercase_columns(df)
    df = add_technical_columns(df, filename, period)

    target_table = TARGET_TABLES[dataset]
    df.write.mode("append").option("mergeSchema", "true").saveAsTable(target_table)
    print(f"OK  {target_table:45s} <- {filename:35s} (sep='{delimiter}', periode={period}, {df.count()} lignes)")


historique_files = [f for f in dbutils.fs.ls(LANDING_HISTO) if f.name.lower().endswith((".csv", ".txt"))]
for f in historique_files:
    ingest_historique_file(f)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingestion des fichiers récents (Parquet, 2025+)
# MAGIC `default_year=2025` couvre le seul fichier observé sans année dans son nom
# MAGIC (`...-1er-trimestre.parquet`) — à confirmer en atelier que ce fichier porte bien sur le 1er trimestre 2025.

# COMMAND ----------

#for t in TARGET_TABLES.values():
#    spark.sql(f"DROP TABLE IF EXISTS {t}")

# COMMAND ----------


def cast_all_to_string(df: DataFrame) -> DataFrame:
    """Les parquets 2025 peuvent avoir des types légèrement différents d'un trimestre à l'autre
    (ex: pourcentage_validations en double vs string), ce qui fait échouer mergeSchema. On
    uniformise en string, cohérent avec le choix déjà fait pour l'historique CSV/TXT."""
    return df.select([F.col(c).cast("string").alias(c) for c in df.columns])

def ingest_recent_file(file_info, default_year: int = 2025) -> None:
    filename = file_info.name
    path = file_info.path
    dataset = classify_dataset(filename)
    period = extract_period(filename, default_year=default_year)

    df = spark.read.parquet(path)
    df = lowercase_columns(df)
    df = cast_all_to_string(df)
    df = add_technical_columns(df, filename, period)

    target_table = TARGET_TABLES[dataset]
    df.write.mode("append").option("mergeSchema", "true").saveAsTable(target_table)
    print(f"OK  {target_table:45s} <- {filename:60s} (periode={period}, {df.count()} lignes)")


recent_files = [f for f in dbutils.fs.ls(LANDING_RECENT) if f.name.lower().endswith(".parquet")]
for f in recent_files:
    ingest_recent_file(f)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Vérification rapide post-ingestion : nombre de périodes couvertes par table

# COMMAND ----------

for table_name in TARGET_TABLES.values():
    display(
        spark.sql(
            f"SELECT _period_label, COUNT(*) AS nb_lignes FROM {table_name} "
            f"GROUP BY _period_label ORDER BY _period_label"
        )
    )