# Databricks notebook source
# MAGIC %md
# MAGIC # Gold — Faits et KPI
# MAGIC Construit les tables de faits unifiées (ferré + surface) et quelques vues de KPI d'exemple, à affiner une
# MAGIC fois les exemples concrets de KPI obtenus en atelier (cf. classeur de cadrage, section 3).

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "idfm_mobilites"

# COMMAND ----------

# MAGIC %md
# MAGIC ## fait_validation_jour (grain : jour x point [arrêt-ou-ligne] x titre x mode_reseau)

# COMMAND ----------

frequentation_ferre = spark.table(f"{CATALOG}.silver.frequentation_ferre").select(
    F.col("jour"),
    F.col("id_arret_jointure").alias("id_point"),
    F.col("id_jointure_type"),
    F.lit(None).cast("string").alias("id_groupe_lignes"),
    F.col("categorie_titre"),
    F.col("nb_validations"),
    F.col("is_truncated"),
    F.col("mode_reseau"),
)

frequentation_surface = spark.table(f"{CATALOG}.silver.frequentation_surface").select(
    F.col("jour"),
    F.lit(None).cast("string").alias("id_point"),
    F.lit(None).cast("string").alias("id_jointure_type"),
    F.col("id_ligne_jointure").alias("id_groupe_lignes"),
    F.col("categorie_titre"),
    F.col("nb_validations"),
    F.col("is_truncated"),
    F.col("mode_reseau"),
)

fait_validation_jour = frequentation_ferre.unionByName(frequentation_surface)
(
    fait_validation_jour.write.mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("mode_reseau")
    .saveAsTable(f"{CATALOG}.gold.fait_validation_jour")
)
print(f"OK  gold.fait_validation_jour : {fait_validation_jour.count()} lignes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## fait_profil_horaire (grain : catégorie jour x tranche horaire x point [arrêt-ou-ligne] x mode_reseau)

# COMMAND ----------

profil_ferre = spark.table(f"{CATALOG}.silver.profil_horaire_ferre").select(
    F.col("id_arret_jointure").alias("id_point"),
    F.col("id_jointure_type"),
    F.lit(None).cast("string").alias("id_groupe_lignes"),
    F.col("cat_jour"),
    F.col("tranche_horaire"),
    F.col("pourcentage_validations"),
    F.col("mode_reseau"),
)

profil_surface = spark.table(f"{CATALOG}.silver.profil_horaire_surface").select(
    F.lit(None).cast("string").alias("id_point"),
    F.lit(None).cast("string").alias("id_jointure_type"),
    F.col("id_ligne_jointure").alias("id_groupe_lignes"),
    F.col("cat_jour"),
    F.col("tranche_horaire"),
    F.col("pourcentage_validations"),
    F.col("mode_reseau"),
)

fait_profil_horaire = profil_ferre.unionByName(profil_surface)
(
    fait_profil_horaire.write.mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("mode_reseau")
    .saveAsTable(f"{CATALOG}.gold.fait_profil_horaire")
)
print(f"OK  gold.fait_profil_horaire : {fait_profil_horaire.count()} lignes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Vues KPI d'exemple
# MAGIC A remplacer/compléter par les KPI réels une fois obtenus en atelier. Objectif ici : démontrer que le
# MAGIC modèle en étoile permet de répondre à ces questions sans table ad hoc dédiée.

# COMMAND ----------

# MAGIC %md
# MAGIC ### KPI 1 — Évolution mensuelle des validations par mode de réseau (ferré vs surface)

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {CATALOG}.gold.kpi_validations_mensuelles_par_mode AS
SELECT
    date_trunc('MONTH', jour) AS mois,
    mode_reseau,
    SUM(COALESCE(nb_validations, 0)) AS total_validations,
    SUM(CASE WHEN is_truncated THEN 1 ELSE 0 END) AS nb_lignes_tronquees_moins_de_5
FROM {CATALOG}.gold.fait_validation_jour
GROUP BY date_trunc('MONTH', jour), mode_reseau
""")
print("OK  vue gold.kpi_validations_mensuelles_par_mode créée")

# COMMAND ----------

# MAGIC %md
# MAGIC ### KPI 2 — Top arrêts ferrés par fréquentation, avec la commune (croisement référentiel x fréquentation)

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {CATALOG}.gold.kpi_top_arrets_ferres AS
SELECT
    a.nom AS nom_arret,
    a.commune,
    a.granularite,
    SUM(COALESCE(f.nb_validations, 0)) AS total_validations
FROM {CATALOG}.gold.fait_validation_jour f
JOIN {CATALOG}.gold.dim_arret_ou_zone a
    ON f.id_point = a.id AND f.id_jointure_type IN ('ARRET', 'ZDC_A_CONFIRMER')
WHERE f.mode_reseau = 'FER'
GROUP BY a.nom, a.commune, a.granularite
ORDER BY total_validations DESC
""")
print("OK  vue gold.kpi_top_arrets_ferres créée")

# COMMAND ----------

# MAGIC %md
# MAGIC ### KPI 3 — Profil horaire moyen par catégorie de jour (pour un mode de réseau donné)

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {CATALOG}.gold.kpi_profil_horaire_moyen AS
SELECT
    mode_reseau,
    cat_jour,
    tranche_horaire,
    AVG(pourcentage_validations) AS pourcentage_validations_moyen
FROM {CATALOG}.gold.fait_profil_horaire
GROUP BY mode_reseau, cat_jour, tranche_horaire
""")
print("OK  vue gold.kpi_profil_horaire_moyen créée")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Rappel garde-fou métier (à intégrer dans tout dashboard / Genie Space)
# MAGIC Les données de validation ne comptabilisent que les entrées badgées sur le réseau (Pass Navigo /
# MAGIC Imagine R / Améthyste / TST / FGT). Elles excluent les tickets magnétiques, les usagers qui ne valident
# MAGIC pas, les fraudeurs, ainsi que les sorties et correspondances. Elles donnent donc une vision **partielle**
# MAGIC du trafic réel — à rappeler dans toute restitution (cf. doc PRIM "Données de validation télébilletiques").