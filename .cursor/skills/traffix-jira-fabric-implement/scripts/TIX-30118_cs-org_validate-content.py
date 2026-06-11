# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse_name": "lh_gold",
# META       "default_lakehouse_workspace_id": "5a2907f0-782e-4dd3-80c2-35e22d411d08"
# META     },
# META     "environment": {}
# META   }
# META }

# MARKDOWN ********************

# # TIX-30118 — CS org / country diagnostics (BEFORE)
# Read-only prod baseline for Customer Sales org membership analysis.

# CELL ********************

%run spark_configuration

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

path_source_dimension = "/Tables/dimension/"
path_source_facts = "/Tables/customer_sales/"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run gold_configuration

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

dim_user = spark.read.load(production_lakehouse_gold_abfss + path_source_dimension + "dim_user")
dim_psr = spark.read.load(production_lakehouse_gold_abfss + path_source_dimension + "dim_primary_sales_rep")
dim_fg = spark.read.load(production_lakehouse_gold_abfss + path_source_dimension + "dim_functional_group")
fact = spark.read.load(production_lakehouse_gold_abfss + path_source_facts + "fact_customer_sales_v2")

print("=== TABLE ROW COUNTS ===")
for name, df in [
    ("dim_user", dim_user),
    ("dim_primary_sales_rep", dim_psr),
    ("dim_functional_group", dim_fg),
    ("fact_customer_sales_v2", fact),
]:
    print(name, df.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("=== dim_primary_sales_rep SCHEMA (country present?) ===")
print([f.name for f in dim_psr.schema.fields])
if "country" in dim_psr.columns:
    dim_psr.groupBy(
        F.when(F.col("country").isNull() | (F.trim(F.col("country")) == ""), F.lit("NULL/BLANK")).otherwise(F.col("country")).alias("country_bucket")
    ).count().orderBy(F.desc("count")).show(50, truncate=False)
else:
    print("country column MISSING on dim_primary_sales_rep")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("=== REASSIGNMENT TARGET KEYS ===")
targets = [
    ("Kiran N", ["kiran"]),
    ("Ryan Lewin", ["ryan", "lewin"]),
    ("Iliyaaz Ali", ["iliyaaz", "ali"]),
    ("Alternate Canada Sales", ["alternate canada"]),
    ("Alternate US Sales", ["alternate us"]),
    ("Back Office", ["back office"]),
]

for label, tokens in targets:
    print(f"\n--- {label} ---")
    dim_user.filter(
        F.lower(F.col("full_name")).rlike("|".join(tokens))
        | F.lower(F.col("email")).rlike("|".join(tokens))
    ).select("user_key", "primary_key", "full_name", "email", "country", "functional_group", "department", "employee_type").show(20, truncate=False)

    dim_fg.filter(F.lower(F.col("functional_group")).rlike("|".join(tokens))).select(
        "functional_group_key", "functional_group", "department", "sales_country", "operations_country"
    ).show(20, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("=== dim_primary_sales_rep.country vs dim_user.country (email join) ===")
if "country" in dim_psr.columns:
    cmp_df = (
        dim_psr.alias("p")
        .join(dim_user.alias("u"), F.lower(F.col("p.email")) == F.lower(F.col("u.email")), "inner")
        .withColumn(
            "country_match",
            F.when(
                F.coalesce(F.trim(F.col("p.country")), F.lit("")) == F.coalesce(F.trim(F.col("u.country")), F.lit("")),
                F.lit("match"),
            ).otherwise(F.lit("mismatch")),
        )
    )
    cmp_df.groupBy("country_match").count().show()
    cmp_df.filter(F.col("country_match") == "mismatch").select(
        "p.user_key", "p.full_name", "p.country", "u.country"
    ).show(20, truncate=False)
    cmp_df.filter(F.col("p.country").isNull() | (F.trim(F.col("p.country")) == "")).count()
    print("dim_primary_sales_rep rows with NULL/blank country (joined to dim_user):", cmp_df.filter(F.col("p.country").isNull() | (F.trim(F.col("p.country")) == "")).count())
else:
    print("Skip compare — country not on dim_primary_sales_rep")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("=== FACT: sales reps + functional groups on fact_customer_sales_v2 ===")
fact_enriched = (
    fact.alias("f")
    .join(dim_psr.alias("psr"), F.col("f.primary_sales_rep_key") == F.col("psr.user_key"), "left")
    .join(dim_fg.alias("fg"), F.col("f.functional_group_key") == F.col("fg.functional_group_key"), "left")
    .join(dim_user.alias("u"), F.col("f.primary_sales_rep_key") == F.col("u.user_key"), "left")
)

print("Distinct sales reps on fact (top 30 by load count):")
fact_enriched.groupBy(
    F.col("psr.user_key"), F.col("psr.full_name"), F.col("psr.email"), F.col("psr.functional_group"), F.col("psr.department"), F.col("u.country")
).agg(F.countDistinct("f.load_key").alias("load_count")).orderBy(F.desc("load_count")).show(30, truncate=False)

print("Distinct functional groups on fact:")
fact_enriched.groupBy(F.col("fg.functional_group"), F.col("fg.department"), F.col("fg.sales_country")).agg(
    F.countDistinct("f.load_key").alias("load_count")
).orderBy(F.desc("load_count")).show(50, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("=== SAMPLE: Iliyaaz Ali loads on fact ===")
fact_enriched.filter(F.lower(F.col("psr.full_name")).contains("iliyaaz")).select(
    "f.load_key", "psr.full_name", "psr.functional_group", "psr.department", "u.country", "fg.functional_group"
).distinct().show(20, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("=== CS ORG RULE DISCOVERY: functional_group x department on fact reps ===")
fact_enriched.groupBy(F.col("psr.functional_group"), F.col("psr.department")).agg(
    F.countDistinct("f.load_key").alias("load_count"),
    F.countDistinct("psr.user_key").alias("rep_count"),
).orderBy(F.desc("load_count")).show(60, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("=== COUNTRY DISTRIBUTION for fact-attributed reps (dim_user.country) ===")
fact_enriched.groupBy(F.coalesce(F.col("u.country"), F.lit("NULL")).alias("country")).agg(
    F.countDistinct("f.load_key").alias("load_count")
).orderBy(F.desc("load_count")).show(30, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
