# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze — Référentiel arrêts et lignes IDFM
# MAGIC Ingestion brute (append-only) des fichiers du référentiel arrêts/lignes déposés dans le volume de landing.
# MAGIC Aucune transformation métier ici : on ajoute uniquement des colonnes techniques de traçabilité.
# MAGIC
# MAGIC Fichiers attendus dans `/Volumes/idfm_mobilites/bronze/landing/referentiel_arrets/` et `referentiel_lignes/` :
# MAGIC arrets-transporteur.parquet, arrets.parquet, zones-d-arrets.parquet, zones-de-correspondance.parquet,
# MAGIC referentiel-des-lignes.parquet, liste-transporteurs.parquet.
# MAGIC
# MAGIC Manquants connus à ce stade (cf. atelier de cadrage) : acces.csv, poles-d-echange.csv, relations.csv,
# MAGIC relations-acces.csv, fiches-horaires-et-plans.csv — non bloquants car les clés étrangères (ArRId, ZdAId,
# MAGIC ZdCId) sont déjà présentes dans les fichiers reçus.

# COMMAND ----------

from pyspark.sql import functions as F, DataFrame

CATALOG = "idfm_mobilites"
LANDING_ARRETS = f"/Volumes/{CATALOG}/bronze/landing/referentiel_arrets"
LANDING_LIGNES = f"/Volumes/{CATALOG}/bronze/landing/referentiel_lignes"

FILES = {
    "arrets_transporteur": (LANDING_ARRETS, "arrets-transporteur.parquet"),
    "arrets_reference": (LANDING_ARRETS, "arrets.parquet"),
    "zones_arrets": (LANDING_ARRETS, "zones-d-arrets.parquet"),
    "zones_correspondance": (LANDING_ARRETS, "zones-de-correspondance.parquet"),
    "lignes": (LANDING_LIGNES, "referentiel-des-lignes.parquet"),
    "transporteurs": (LANDING_LIGNES, "liste-transporteurs.parquet"),
}

# COMMAND ----------


def add_technical_columns(df: DataFrame, source_file: str) -> DataFrame:
    return (
        df.withColumn("_source_file", F.lit(source_file))
        .withColumn("_ingested_at", F.current_timestamp())
    )


def ingest_referentiel_file(table_name: str, folder: str, filename: str) -> None:
    source_path = f"{folder}/{filename}"
    df = spark.read.parquet(source_path)
    df = add_technical_columns(df, filename)
    target_table = f"{CATALOG}.bronze.{table_name}"
    df.write.mode("append").saveAsTable(target_table)
    print(f"OK  {target_table:45s} <- {filename:35s} ({df.count()} lignes)")


# COMMAND ----------

for table_name, (folder, filename) in FILES.items():
    ingest_referentiel_file(table_name, folder, filename)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Vérification rapide post-ingestion

# COMMAND ----------

for table_name in FILES:
    display(spark.sql(f"SELECT COUNT(*) AS nb_lignes, MAX(_ingested_at) AS derniere_ingestion FROM {CATALOG}.bronze.{table_name}"))