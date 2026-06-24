# Databricks notebook source
# MAGIC %md
# MAGIC # Gold — Dimensions
# MAGIC Construit le modèle en étoile à partir des tables Silver. Point d'attention repris du notebook Silver
# MAGIC fréquentation : pour le réseau ferré, la clé de jointure des validations peut désigner soit un **Arrêt
# MAGIC de référence**, soit potentiellement une **Zone De Correspondance** selon la période (`id_jointure_type`).
# MAGIC Tant que ce point n'est pas tranché en atelier, `dim_arret_ou_zone` unifie les deux référentiels avec un
# MAGIC discriminant `granularite`, pour que la jointure du fait reste correcte dans tous les cas.

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "idfm_mobilites"

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_temps
# MAGIC Calendrier couvrant 2015 à aujourd'hui + 1 an, avec la catégorie de jour IDFM (JOHV/SAHV/JOVS/SAVS/DIJFP
# MAGIC approximée ici par jour de semaine + jours fériés FR ; à recaler avec la définition exacte d'IDFM si les
# MAGIC vacances scolaires doivent être prises en compte précisément — cf. doc PRIM, catégories calculées par
# MAGIC semestre sur le réel constaté).

# COMMAND ----------

dim_temps = (
    spark.sql("SELECT explode(sequence(to_date('2015-01-01'), date_add(current_date(), 365), interval 1 day)) AS jour")
    .withColumn("annee", F.year("jour"))
    .withColumn("mois", F.month("jour"))
    .withColumn("jour_semaine", F.dayofweek("jour"))  # 1=dimanche ... 7=samedi
    .withColumn("nom_jour_semaine", F.date_format("jour", "EEEE"))
    .withColumn("est_weekend", F.col("jour_semaine").isin(1, 7))
)
dim_temps.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.gold.dim_temps")
print(f"OK  gold.dim_temps : {dim_temps.count()} lignes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_arret_ou_zone (unifie arrêt de référence et zone de correspondance)

# COMMAND ----------

silver_arret_reference = spark.table(f"{CATALOG}.silver.arret_reference").select(
    F.col("id"),
    F.col("nom"),
    F.col("commune"),
    F.col("code_insee_commune"),
    F.col("mode"),
    F.col("zone_tarifaire"),
    F.col("x_lambert93"),
    F.col("y_lambert93"),
    F.lit("ARRET").alias("granularite"),
)

silver_zone_correspondance = spark.table(f"{CATALOG}.silver.zone_correspondance").select(
    F.col("id"),
    F.col("nom"),
    F.col("commune"),
    F.col("code_insee_commune"),
    F.col("mode_dominant").alias("mode"),
    F.lit(None).cast("string").alias("zone_tarifaire"),
    F.col("x_lambert93_centroide").alias("x_lambert93"),
    F.col("y_lambert93_centroide").alias("y_lambert93"),
    F.lit("ZDC_A_CONFIRMER").alias("granularite"),
)

dim_arret_ou_zone = silver_arret_reference.unionByName(silver_zone_correspondance)
dim_arret_ou_zone.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{CATALOG}.gold.dim_arret_ou_zone"
)
print(f"OK  gold.dim_arret_ou_zone : {dim_arret_ou_zone.count()} lignes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_ligne

# COMMAND ----------

dim_ligne = (
    spark.table(f"{CATALOG}.silver.ligne")
    .select(
        F.col("groupe_lignes_id").alias("id_groupe_lignes"),
        F.col("id").alias("id_ligne_commerciale_exemple"),
        F.col("nom"),
        F.col("nom_court"),
        F.col("mode"),
        F.col("sous_mode"),
        F.col("transporteur_id"),
        F.col("reseau_commercial"),
        F.col("statut"),
    )
    .groupBy("id_groupe_lignes")
    .agg(
        F.first("id_ligne_commerciale_exemple").alias("id_ligne_commerciale_exemple"),
        F.first("nom").alias("nom"),
        F.first("nom_court").alias("nom_court"),
        F.first("mode").alias("mode"),
        F.first("sous_mode").alias("sous_mode"),
        F.first("transporteur_id").alias("transporteur_id"),
        F.first("reseau_commercial").alias("reseau_commercial"),
        F.first("statut").alias("statut"),
    )
    .filter(F.col("id_groupe_lignes").isNotNull())
)

dim_ligne.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.gold.dim_ligne")
print(f"OK  gold.dim_ligne : {dim_ligne.count()} lignes")
print(
    "NB: dim_ligne est au grain 'ligne administrative' (id_groupe_lignes), car c'est la clé portée par "
    "les données de validation (cf. doc PRIM — ID_GroupOfLines). Une ligne administrative peut regrouper "
    "plusieurs lignes commerciales ; on n'en garde qu'un exemplaire (nom/mode) via first()."
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_transporteur

# COMMAND ----------

dim_transporteur = spark.table(f"{CATALOG}.silver.transporteur")
dim_transporteur.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{CATALOG}.gold.dim_transporteur"
)
print(f"OK  gold.dim_transporteur : {dim_transporteur.count()} lignes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_titre_transport
# MAGIC Référentiel des catégories de titre documentées par IDFM (doc PRIM), recopié en dur car ce n'est pas
# MAGIC un jeu de données Open Data à part — seulement 7 valeurs, stables.

# COMMAND ----------

dim_titre_transport = spark.createDataFrame(
    [
        ("IMAGINE R", "Forfaits annuels Imagine R Scolaire / Etudiant"),
        ("NAVIGO", "Forfaits Navigo Annuel / Mois / Semaine"),
        ("AMETHYSTE", "Forfait personnes âgées ou handicapées sous conditions"),
        ("TST", "Réduction Solidarité Transport (hebdo/mensuel)"),
        ("FGT", "Forfait Navigo Gratuité Transport"),
        ("AUTRE TITRE", "Forfaits spéciaux"),
        ("NON DEFINI", "Titre non défini (anomalie)"),
    ],
    schema=["categorie_titre", "description"],
)
dim_titre_transport.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{CATALOG}.gold.dim_titre_transport"
)
print(f"OK  gold.dim_titre_transport : {dim_titre_transport.count()} lignes")