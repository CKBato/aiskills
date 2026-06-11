# Fabric notebook source — append as PROD POST-MERGE section to TIX-{key}_*_validate
# READ-ONLY: production_lakehouse_* only. Run in validation workspace, NOT Traffix-Medallion-Production.

# MARKDOWN ********************
# ## PROD POST-MERGE (read-only) — TIX-{key}
# Run after PR merge + prod full reload. Customize table, assertions, sample per ticket.

# CELL ********************
# %run spark_configuration
# %run silver_configuration  # or gold_configuration

# CELL ********************
# from pyspark.sql import functions as F
#
# # --- Playbook B example: customize path + assertions ---
# prod_path = production_lakehouse_silver_abfss + "/Tables/{schema}/{table}"
# prod_df = spark.read.format("delta").load(prod_path)
#
# bad_cohort = prod_df.where({bad_cohort_expr}).count()
# print(f"PROD bad_cohort: {bad_cohort}")
# assert bad_cohort == 0, f"FAIL: prod bad_cohort = {bad_cohort}"
#
# print("PROD distribution:")
# prod_df.groupBy("{column}").count().orderBy(F.desc("count")).show(20, truncate=False)
#
# print("PROD Jira sample:")
# prod_df.where(F.col("{sample_key}") == "{sample_value}").show(5, truncate=False)
#
# print("PROD grain:")
# print(f"rows={prod_df.count()} distinct={prod_df.select('{grain_col}').distinct().count()}")
# print("PASS: post-prod read-only validation")
