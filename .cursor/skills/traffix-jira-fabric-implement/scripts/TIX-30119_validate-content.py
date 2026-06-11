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

# # TIX-30119 — post-fix validation (validation WS fact)

# CELL ********************

%run spark_configuration

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

path_dim = "/Tables/dimension/"
path_fact = "/Tables/customer_sales/"

fact = spark.read.load(current_lakehouse_gold_abfss + path_fact + "fact_customer_sales_v2")
psr = spark.read.load(production_lakehouse_gold_abfss + path_dim + "dim_primary_sales_rep")
fg = spark.read.load(production_lakehouse_gold_abfss + path_dim + "dim_functional_group")

enriched = (
    fact.alias("f")
    .join(psr.alias("p"), F.col("f.primary_sales_rep_key") == F.col("p.user_key"), "left")
    .join(fg.alias("g"), F.col("f.functional_group_key") == F.col("g.functional_group_key"), "left")
)

print("=== AFTER: Sales Support 1 loads (expect 0 on fg 62) ===")
enriched.filter(F.col("g.functional_group") == "Sales Support 1").count()
print("fg62 count:", enriched.filter(F.col("f.functional_group_key") == 62).count())

print("=== AFTER: Sales Support loads reassigned to Ryan / fg 2 ===")
enriched.filter(F.col("f.functional_group_key") == 2).filter(
    F.col("f.primary_sales_rep_key") == 3089
).count()

print("=== AFTER: Ryan Lewin natural loads (expect ~21343) ===")
enriched.filter(F.col("f.primary_sales_rep_key") == 3089).filter(
    F.col("g.functional_group") == "US Sales 1"
).count()

print("=== AFTER: Non-CS excluded FGs on fact ===")
excluded = ["Sales Support 1", "ALTERNATE SALES CANADA", "ALTERNATE SALES USA"]
for name in excluded:
    c = enriched.filter(F.col("g.functional_group") == name).filter(
        ~F.col("f.primary_sales_rep_key").isin(2096, 3089)
    ).count()
    print(name, "non-target rep rows:", c)

print("=== AFTER: primary_sales_rep_key distribution (top) ===")
enriched.groupBy("f.primary_sales_rep_key", "p.full_name", "g.functional_group").count().orderBy(
    F.desc("count")
).show(15, truncate=False)

print("=== AFTER: functional_group_key distribution (top) ===")
enriched.groupBy("f.functional_group_key", "g.functional_group").count().orderBy(F.desc("count")).show(
    15, truncate=False
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
