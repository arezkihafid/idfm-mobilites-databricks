# Databricks notebook source
# MAGIC %md
# MAGIC # Setup — Catalogue et schémas IDFM Mobilités
# MAGIC Crée le catalogue Unity Catalog dédié au Data Product et ses schémas (bronze/silver/gold/sandbox/silver_config),
# MAGIC ainsi que le volume de dépôt manuel des fichiers source (landing zone).
# MAGIC
# MAGIC A exécuter une seule fois (ou en tant que step "setup" d'un Workflow), avec un principal ayant le droit
# MAGIC CREATE CATALOG sur le metastore.

# COMMAND ----------

CATALOG = "idfm_mobilites"

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")

for schema in ["bronze", "silver", "silver_config", "gold", "sandbox"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Volume de dépôt (landing zone)
# MAGIC Les fichiers sont déposés manuellement dans ce volume, organisés par sous-dossier et horodatés à chaque dépôt
# MAGIC (cf. convention de nommage dans le README du projet).

# COMMAND ----------

spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.bronze.landing")

for sub in ["referentiel_arrets", "referentiel_lignes", "frequentation/historique", "frequentation/recent"]:
    dbutils.fs.mkdirs(f"/Volumes/{CATALOG}/bronze/landing/{sub}")

# COMMAND ----------

# MAGIC %md
# MAGIC Arborescence attendue sous `/Volumes/idfm_mobilites/bronze/landing/` :
# MAGIC ```
# MAGIC referentiel_arrets/        <- arrets-transporteur.parquet, arrets.parquet, zones-d-arrets.parquet, zones-de-correspondance.parquet
# MAGIC referentiel_lignes/        <- referentiel-des-lignes.parquet, liste-transporteurs.parquet
# MAGIC frequentation/historique/  <- *.csv / *.txt (2015-2024, formats hétérogènes, cf. notebook 02_bronze_frequentation)
# MAGIC frequentation/recent/      <- *.parquet (2025+)
# MAGIC ```