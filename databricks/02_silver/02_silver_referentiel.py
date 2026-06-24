# Databricks notebook source
# MAGIC %md
# MAGIC # Silver — Référentiel arrêts et lignes
# MAGIC Le Bronze est alimenté en *append* à chaque exécution (MAJ quotidienne IDFM = snapshot complet rejoué).
# MAGIC Cette couche déduplique pour ne garder que la **dernière version connue** de chaque objet (stratégie
# MAGIC SCD Type 1 — pas d'historisation des changements). Si l'atelier de cadrage confirme un besoin
# MAGIC d'historiser les évolutions du référentiel (cf. classeur de cadrage, section 3), cette logique sera
# MAGIC remplacée par un SCD Type 2 (ajout de `valid_from`/`valid_to`/`is_current`).

# COMMAND ----------

from pyspark.sql import functions as F, DataFrame, Window

CATALOG = "idfm_mobilites"

# COMMAND ----------


def keep_latest_version(df: DataFrame, key_col: str, version_col: str, changed_col: str) -> DataFrame:
    """Garde uniquement la ligne la plus récente par clé naturelle, en s'appuyant d'abord sur le
    numéro de version IDFM, puis sur la date de dernière modification en cas d'égalité."""
    window = Window.partitionBy(key_col).orderBy(
        F.col(version_col).desc(), F.col(changed_col).desc(), F.col("_ingested_at").desc()
    )
    return (
        df.withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## Arrêts transporteurs

# COMMAND ----------

bronze_arret_transporteur = spark.table(f"{CATALOG}.bronze.arrets_transporteur")
silver_arret_transporteur = keep_latest_version(
    bronze_arret_transporteur, "artid", "artversion", "artchanged"
).select(
    F.col("artid").alias("id"),
    F.expr("try_cast(artversion as int)").alias("version"),
    F.trim(F.col("artname")).alias("nom"),
    F.col("artxepsg2154").cast("double").alias("x_lambert93"),
    F.col("artyepsg2154").cast("double").alias("y_lambert93"),
    F.trim(F.col("arttown")).alias("commune"),
    F.col("artpostalregion").alias("code_insee_commune"),
    F.trim(F.col("arttype")).alias("mode"),
    F.col("artfarezone").alias("zone_tarifaire"),
    F.col("arrid").alias("arret_reference_id"),
    F.col("artgeopoint").alias("geopoint"),
    F.col("artchanged").alias("derniere_modification"),
)
silver_arret_transporteur.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{CATALOG}.silver.arret_transporteur"
)
print(f"OK  silver.arret_transporteur : {silver_arret_transporteur.count()} lignes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Arrêts de référence

# COMMAND ----------

bronze_arret_reference = spark.table(f"{CATALOG}.bronze.arrets_reference")
silver_arret_reference = keep_latest_version(
    bronze_arret_reference, "arrid", "arrversion", "arrchanged"
).select(
    F.col("arrid").alias("id"),
    F.expr("try_cast(arrversion as int)").alias("version"),
    F.trim(F.col("arrname")).alias("nom"),
    F.col("arrxepsg2154").cast("double").alias("x_lambert93"),
    F.col("arryepsg2154").cast("double").alias("y_lambert93"),
    F.trim(F.col("arrtown")).alias("commune"),
    F.col("arrpostalregion").alias("code_insee_commune"),
    F.trim(F.col("arrtype")).alias("mode"),
    F.col("arrfarezone").alias("zone_tarifaire"),
    F.col("zdaid").alias("zone_arrets_id"),
    F.col("arrgeopoint").alias("geopoint"),
    F.col("arrchanged").alias("derniere_modification"),
)
silver_arret_reference.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{CATALOG}.silver.arret_reference"
)
print(f"OK  silver.arret_reference : {silver_arret_reference.count()} lignes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Zones d'arrêts

# COMMAND ----------

bronze_zones_arrets = spark.table(f"{CATALOG}.bronze.zones_arrets")
silver_zones_arrets = keep_latest_version(
    bronze_zones_arrets, "zdaid", "zdaversion", "zdachanged"
).select(
    F.col("zdaid").alias("id"),
    F.expr("try_cast(zdaversion as int)").alias("version"),
    F.trim(F.col("zdaname")).alias("nom"),
    F.col("zdaxepsg2154").cast("double").alias("x_lambert93_centroide"),
    F.col("zdayepsg2154").cast("double").alias("y_lambert93_centroide"),
    F.trim(F.col("zdatown")).alias("commune"),
    F.col("zdapostalregion").alias("code_insee_commune"),
    F.trim(F.col("zdatype")).alias("mode"),
    F.col("zdcid").alias("zone_correspondance_id"),
    F.col("zdachanged").alias("derniere_modification"),
)
silver_zones_arrets.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{CATALOG}.silver.zone_arrets"
)
print(f"OK  silver.zone_arrets : {silver_zones_arrets.count()} lignes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Zones de correspondance

# COMMAND ----------

bronze_zones_correspondance = spark.table(f"{CATALOG}.bronze.zones_correspondance")
silver_zones_correspondance = keep_latest_version(
    bronze_zones_correspondance, "zdcid", "zdcversion", "zdcchanged"
).select(
    F.col("zdcid").alias("id"),
    F.expr("try_cast(zdcversion as int)").alias("version"),
    F.trim(F.col("zdcname")).alias("nom"),
    F.col("zdcxepsg2154").cast("double").alias("x_lambert93_centroide"),
    F.col("zdcyepsg2154").cast("double").alias("y_lambert93_centroide"),
    F.trim(F.col("zdctown")).alias("commune"),
    F.col("zdcpostalregion").alias("code_insee_commune"),
    F.trim(F.col("zdctype")).alias("mode_dominant"),
    F.col("zdcchanged").alias("derniere_modification"),
)
silver_zones_correspondance.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{CATALOG}.silver.zone_correspondance"
)
print(f"OK  silver.zone_correspondance : {silver_zones_correspondance.count()} lignes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lignes
# MAGIC Pas de colonne de version applicable de la même façon (`valid_fromDate`/`status`) : on déduplique sur
# MAGIC `id_line` en gardant la ligne `Status = 'Active'` la plus récemment ingérée si plusieurs versions coexistent.

# COMMAND ----------

bronze_lignes = spark.table(f"{CATALOG}.bronze.lignes")
window_lignes = Window.partitionBy("id_line").orderBy(
    F.when(F.upper(F.col("status")) == "ACTIVE", 0).otherwise(1), F.col("_ingested_at").desc()
)
silver_lignes = (
    bronze_lignes.withColumn("_rn", F.row_number().over(window_lignes))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
    .select(
        F.col("id_line").alias("id"),
        F.trim(F.col("name_line")).alias("nom"),
        F.trim(F.col("shortname_line")).alias("nom_court"),
        F.trim(F.col("transportmode")).alias("mode"),
        F.trim(F.col("transportsubmode")).alias("sous_mode"),
        F.trim(F.col("operatorref")).alias("transporteur_id"),
        F.trim(F.col("operatorname")).alias("transporteur_nom"),
        F.trim(F.col("networkname")).alias("reseau_commercial"),
        F.col("id_groupoflines").alias("groupe_lignes_id"),
        F.col("valid_fromdate").alias("date_debut_activation"),
        F.col("valid_todate").alias("date_fin_activation"),
        F.upper(F.trim(F.col("status"))).alias("statut"),
    )
)
silver_lignes.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.silver.ligne")
print(f"OK  silver.ligne : {silver_lignes.count()} lignes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transporteurs

# COMMAND ----------

bronze_transporteurs = spark.table(f"{CATALOG}.bronze.transporteurs")
silver_transporteurs = bronze_transporteurs.select(
    F.trim(F.col("operatorref")).alias("id"),
    F.trim(F.col("operatorname")).alias("nom"),
    F.trim(F.col("town")).alias("ville"),
    F.trim(F.col("phone")).alias("telephone"),
    F.trim(F.col("url")).alias("site_web"),
    F.trim(F.col("email")).alias("email"),
).dropDuplicates(["id"])
silver_transporteurs.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{CATALOG}.silver.transporteur"
)
print(f"OK  silver.transporteur : {silver_transporteurs.count()} lignes")