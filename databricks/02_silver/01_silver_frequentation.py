# Databricks notebook source
# MAGIC %md
# MAGIC # Silver — Fréquentation (validations télébilletiques)
# MAGIC Normalise les 4 tables Bronze brutes vers un schéma cible **stable**, malgré la dérive de schéma
# MAGIC observée sur 10 ans (2015-2025) :
# MAGIC
# MAGIC | Champ logique | Dataset | Alias rencontrés selon la période |
# MAGIC |---|---|---|
# MAGIC | id_arret_jointure | NB_FER | `id_refa_lda` (2015→2023S1), `id_zdc` (2023S2→2024), `ida` (2025, en double) |
# MAGIC | id_arret_jointure | PROFIL_FER | `id_refa_lda` (2015→2022S1), `lda` (2022S2→2023S1), `id_zdc` (2023S2→2024), `ida` (2025) |
# MAGIC | id_ligne_jointure | NB_SURFACE | `id_groupoflines` (stable sur toute la période) |
# MAGIC | id_ligne_jointure | PROFIL_SURFACE | `id_groupofligne` (typo IDFM, stable sur toute la période, y compris en 2025) |
# MAGIC | pourcentage_validations | PROFIL_FER / PROFIL_SURFACE | `pourc_validations` (jusqu'à 2024S1), `pourcentage_validations` (à partir de 2024T3) |
# MAGIC
# MAGIC **Choix de conception** : plutôt qu'une table de mapping pilotée par plage de dates (complexe à maintenir et
# MAGIC sujette à erreur sur les dates de bascule exactes), on utilise un **coalesce des alias connus**. Comme
# MAGIC chaque ligne ne renseigne qu'un seul des alias pour sa période (les autres valent NULL après le
# MAGIC `mergeSchema` en Bronze), le coalesce sélectionne naturellement la bonne valeur sans dépendre de dates
# MAGIC précises. Si IDFM introduit un nouvel alias inconnu, le coalesce renverra NULL pour ces lignes — à
# MAGIC surveiller via le contrôle qualité en bas de notebook.

# COMMAND ----------

from pyspark.sql import functions as F, DataFrame

CATALOG = "idfm_mobilites"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fonctions utilitaires communes

# COMMAND ----------


def coalesce_if_exists(df: DataFrame, *candidates: str):
    """Coalesce uniquement sur les colonnes réellement présentes dans le DataFrame,
    pour éviter une AnalysisException si un alias n'a jamais été rencontré."""
    existing = [F.col(c) for c in candidates if c in df.columns]
    if not existing:
        raise ValueError(f"Aucun des alias {candidates} n'existe dans le DataFrame ({df.columns})")
    return F.coalesce(*existing)


def jointure_type_arret(df: DataFrame):
    """IMPORTANT : 'id_zdc' (2023S2+) pourrait désigner une Zone De Correspondance et non un Arrêt
    de référence — ce n'est pas confirmé, ça pourrait être un simple renommage de colonne sans
    changement de sémantique. On trace explicitement quel alias a produit la valeur plutôt que de
    fusionner silencieusement, pour permettre à Gold de joindre vers la bonne table de référentiel
    une fois ce point tranché en atelier (cf. classeur de cadrage)."""
    cases = []
    if "id_refa_lda" in df.columns:
        cases.append((F.col("id_refa_lda").isNotNull(), F.lit("ARRET")))
    if "lda" in df.columns:
        cases.append((F.col("lda").isNotNull(), F.lit("ARRET")))
    if "id_zdc" in df.columns:
        cases.append((F.col("id_zdc").isNotNull(), F.lit("ZDC_A_CONFIRMER")))
    if "ida" in df.columns:
        cases.append((F.col("ida").isNotNull(), F.lit("INCONNU_2025")))
    expr = F.lit(None).cast("string")
    for condition, value in reversed(cases):
        expr = F.when(condition, value).otherwise(expr)
    return expr


def parse_jour(col):
    return F.coalesce(
        F.expr(f"try_to_date({col._jc.toString() if False else 'jour'}, 'dd/MM/yyyy')"),
        F.expr(f"try_to_date({col._jc.toString() if False else 'jour'}, 'dd/MM/yy')"),
    )


def clean_nb_validations(col):
    """NB_VALD est soit un entier (avec parfois des espaces parasites : '2 093'), soit le texte
    'Moins de 5' lorsque la valeur réelle est < 5 (anonymisation RGPD, cf. doc PRIM). Certaines
    valeurs portent aussi un suffixe décimal parasite ('71324.0') — cast via double puis long
    pour les tolérer sans lever d'exception en mode ANSI."""
    cleaned = F.regexp_replace(F.trim(col), " ", "")
    is_truncated = F.lower(cleaned).rlike("moinsde5")
    nb_validations = F.when(is_truncated, F.lit(None)).otherwise(cleaned.cast("double").cast("long"))
    return nb_validations, is_truncated


def normalize_categorie_titre(col):
    """Harmonise la casse et les variantes de libellé ('AUTRE TITRE' / 'Autres titres') observées
    selon les périodes vers les catégories canoniques documentées par IDFM (cf. doc PRIM)."""
    upper = F.upper(F.trim(col))
    return F.when(upper == "AUTRES TITRES", F.lit("AUTRE TITRE")).otherwise(upper)


# COMMAND ----------

# MAGIC %md
# MAGIC ## NB_FER -> silver.frequentation_ferre (grain : jour x arrêt x titre)

# COMMAND ----------

bronze_nb_fer = spark.table(f"{CATALOG}.bronze.frequentation_ferre_raw")

id_arret = coalesce_if_exists(bronze_nb_fer, "id_refa_lda", "id_zdc")
if "ida" in bronze_nb_fer.columns:
    id_arret = F.coalesce(id_arret, F.col("ida").cast("float").cast("long").cast("string"))

nb_validations, is_truncated = clean_nb_validations(F.col("nb_vald"))

silver_frequentation_ferre = bronze_nb_fer.select(
    parse_jour(F.col("jour")).alias("jour"),
    F.col("code_stif_trns").cast("string").alias("transporteur_id"),
    F.col("code_stif_res").cast("string").alias("reseau_id"),
    F.trim(F.col("code_stif_arret")).cast("string").alias("arret_code_stif"),
    F.trim(F.col("libelle_arret")).alias("arret_libelle"),
    F.trim(id_arret).alias("id_arret_jointure"),
    jointure_type_arret(bronze_nb_fer).alias("id_jointure_type"),
    normalize_categorie_titre(F.col("categorie_titre")).alias("categorie_titre"),
    nb_validations.alias("nb_validations"),
    is_truncated.alias("is_truncated"),
    F.lit("FER").alias("mode_reseau"),
    F.col("_source_file"),
    F.col("_period_label"),
)

(
    silver_frequentation_ferre.write.mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.silver.frequentation_ferre")
)
print(f"OK  silver.frequentation_ferre : {silver_frequentation_ferre.count()} lignes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## NB_SURFACE -> silver.frequentation_surface (grain : jour x ligne x titre)

# COMMAND ----------

bronze_nb_surface = spark.table(f"{CATALOG}.bronze.frequentation_surface_raw")

id_ligne = coalesce_if_exists(bronze_nb_surface, "id_groupoflines")
nb_validations, is_truncated = clean_nb_validations(F.col("nb_vald"))

silver_frequentation_surface = bronze_nb_surface.select(
    parse_jour(F.col("jour")).alias("jour"),
    F.col("code_stif_trns").cast("string").alias("transporteur_id"),
    F.col("code_stif_res").cast("string").alias("reseau_id"),
    F.trim(F.col("code_stif_ligne")).cast("string").alias("ligne_code_stif"),
    F.trim(F.col("libelle_ligne")).alias("ligne_libelle"),
    F.trim(id_ligne).alias("id_ligne_jointure"),
    normalize_categorie_titre(F.col("categorie_titre")).alias("categorie_titre"),
    nb_validations.alias("nb_validations"),
    is_truncated.alias("is_truncated"),
    F.lit("SURFACE").alias("mode_reseau"),
    F.col("_source_file"),
    F.col("_period_label"),
)

(
    silver_frequentation_surface.write.mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.silver.frequentation_surface")
)
print(f"OK  silver.frequentation_surface : {silver_frequentation_surface.count()} lignes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## PROFIL_FER -> silver.profil_horaire_ferre (grain : arrêt x catégorie jour x tranche horaire)

# COMMAND ----------

bronze_profil_fer = spark.table(f"{CATALOG}.bronze.profil_ferre_raw")

id_arret = coalesce_if_exists(bronze_profil_fer, "id_refa_lda", "lda", "id_zdc")
if "ida" in bronze_profil_fer.columns:
    id_arret = F.coalesce(id_arret, F.col("ida").cast("double").cast("long").cast("string"))

pourcentage = coalesce_if_exists(bronze_profil_fer, "pourc_validations", "pourcentage_validations")

silver_profil_horaire_ferre = bronze_profil_fer.select(
    F.col("code_stif_trns").cast("string").alias("transporteur_id"),
    F.col("code_stif_res").cast("string").alias("reseau_id"),
    F.trim(F.col("code_stif_arret")).cast("string").alias("arret_code_stif"),
    F.trim(F.col("libelle_arret")).alias("arret_libelle"),
    F.trim(id_arret).alias("id_arret_jointure"),
    jointure_type_arret(bronze_profil_fer).alias("id_jointure_type"),
    F.upper(F.trim(F.col("cat_jour"))).alias("cat_jour"),
    F.trim(F.col("trnc_horr_60")).alias("tranche_horaire"),
    F.regexp_replace(pourcentage, ",", ".").cast("double").alias("pourcentage_validations"),
    F.lit("FER").alias("mode_reseau"),
    F.col("_source_file"),
    F.col("_period_label"),
)

(
    silver_profil_horaire_ferre.write.mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.silver.profil_horaire_ferre")
)
print(f"OK  silver.profil_horaire_ferre : {silver_profil_horaire_ferre.count()} lignes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## PROFIL_SURFACE -> silver.profil_horaire_surface (grain : ligne x catégorie jour x tranche horaire)
# MAGIC Note : la colonne `id_groupofligne` (avec la typo IDFM) est l'unique alias rencontré, y compris dans
# MAGIC les parquets 2025. Les colonnes `code_tlb_*` apparues une seule fois (2024 T4) ne sont pas reprises ici,
# MAGIC car non documentées et non présentes ailleurs dans l'historique — à clarifier en atelier si besoin.

# COMMAND ----------

bronze_profil_surface = spark.table(f"{CATALOG}.bronze.profil_surface_raw")

id_ligne = coalesce_if_exists(bronze_profil_surface, "id_groupofligne")
pourcentage = coalesce_if_exists(bronze_profil_surface, "pourc_validations", "pourcentage_validations")

silver_profil_horaire_surface = bronze_profil_surface.select(
    F.col("code_stif_trns").cast("string").alias("transporteur_id"),
    F.col("code_stif_res").cast("string").alias("reseau_id"),
    F.trim(F.col("code_stif_ligne")).cast("string").alias("ligne_code_stif"),
    F.trim(F.col("libelle_ligne")).alias("ligne_libelle"),
    F.trim(id_ligne).alias("id_ligne_jointure"),
    F.upper(F.trim(F.col("cat_jour"))).alias("cat_jour"),
    F.trim(F.col("trnc_horr_60")).alias("tranche_horaire"),
    F.regexp_replace(pourcentage, ",", ".").cast("double").alias("pourcentage_validations"),
    F.lit("SURFACE").alias("mode_reseau"),
    F.col("_source_file"),
    F.col("_period_label"),
)

(
    silver_profil_horaire_surface.write.mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.silver.profil_horaire_surface")
)
print(f"OK  silver.profil_horaire_surface : {silver_profil_horaire_surface.count()} lignes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Contrôle qualité : lignes où id_arret_jointure / id_ligne_jointure est resté NULL
# MAGIC Si ce contrôle remonte des lignes, c'est qu'un nouvel alias de colonne est apparu et n'est pas
# MAGIC encore couvert par les `coalesce` ci-dessus — à corriger avant de poursuivre vers Gold.

# COMMAND ----------

for table_name, key_col in [
    ("silver.frequentation_ferre", "id_arret_jointure"),
    ("silver.frequentation_surface", "id_ligne_jointure"),
    ("silver.profil_horaire_ferre", "id_arret_jointure"),
    ("silver.profil_horaire_surface", "id_ligne_jointure"),
]:
    nb_null = spark.table(f"{CATALOG}.{table_name}").filter(F.col(key_col).isNull()).count()
    statut = "OK" if nb_null == 0 else "A INVESTIGUER"
    print(f"{statut:15s} {table_name:35s} : {nb_null} lignes avec {key_col} NULL")