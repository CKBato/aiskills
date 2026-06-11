# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse_name": "lh_silver",
# META       "default_lakehouse_workspace_id": "5a2907f0-782e-4dd3-80c2-35e22d411d08"
# META     },
# META     "environment": {}
# META   }
# META }

# MARKDOWN ********************

# # TIX-30027 — post-prod validation (read-only prod lakehouses)

# CELL ********************

%run spark_configuration

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run silver_configuration

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run bronze_netsuite_configuration

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

path = "/Tables/netsuite/"
table = "transaction_accounting_line"
sample_pk = "16845487|25|1"
txn_id = 16845487

print("=== TIX-30027 POST-PROD VALIDATION (read-only) ===")

prod_silver = spark.read.load(production_lakehouse_silver_abfss + path + table)
prod_bronze_tal = spark.read.load(production_lakehouse_bronze_abfss + path + "transactionaccountingline")

# R1 table readable
silver_count = prod_silver.count()
bronze_tal_count = prod_bronze_tal.count()
print(f"R1 table readable: PASS — silver rows={silver_count:,}, bronze TAL rows={bronze_tal_count:,}")

# Primary assertion: silver active missing active bronze TAL
orphan_count = spark.sql(
    """
    select count(*) as c
    from {silver} s
    left join {bronze} b
      on s.primary_key = b.primary_key and b.__sys_deleted = false
    where s.__sys_deleted = false and b.primary_key is null
    """,
    silver=prod_silver,
    bronze=prod_bronze_tal,
).first()[0]
print(f"Primary orphan check (silver active missing bronze TAL): count={orphan_count}")
if orphan_count != 0:
    raise Exception(f"FAIL: Expected 0 silver-active rows missing bronze TAL, got {orphan_count}")

# R3 grain
active = prod_silver.where(F.col("__sys_deleted") == False)
grain_total = active.count()
grain_distinct = active.select("primary_key").distinct().count()
print(f"R3 grain: total_active={grain_total:,}, distinct_pk={grain_distinct:,}")
if grain_total != grain_distinct:
    raise Exception(f"FAIL: Grain integrity failed on active silver rows ({grain_total} vs {grain_distinct})")

# Sample PK status
sample = prod_silver.where(F.col("primary_key") == sample_pk)
sample_count = sample.count()
print(f"Sample PK {sample_pk}: row_count={sample_count}")
if sample_count > 0:
    sample.select(
        "primary_key",
        "id",
        "title",
        "transaction_line",
        "department_name",
        "cost_center",
        "debit_gl_line",
        "credit_gl_line",
        "__sys_deleted",
        "__sys_synced",
    ).show(truncate=False)
else:
    print(f"Sample PK {sample_pk}: not present in prod silver (may be tombstoned/removed)")

bronze_sample = prod_bronze_tal.where(F.col("primary_key") == sample_pk)
bronze_sample_count = bronze_sample.count()
print(f"Bronze TAL sample PK {sample_pk}: row_count={bronze_sample_count}")
if bronze_sample_count > 0:
    bronze_sample.select("primary_key", "transaction", "transactionline", "__sys_deleted", "__sys_synced").show(
        truncate=False
    )

# Txn-level active line count for context
txn_active_silver = active.where(F.col("id") == txn_id).count()
txn_active_bronze = prod_bronze_tal.where((F.col("transaction") == txn_id) & (F.col("__sys_deleted") == False)).count()
print(f"Txn {txn_id} active lines: silver={txn_active_silver}, bronze TAL={txn_active_bronze}")

results = [
    ("orphan_count", str(orphan_count), "PASS" if orphan_count == 0 else "FAIL"),
    ("grain_total", str(grain_total), "PASS"),
    ("grain_distinct", str(grain_distinct), "PASS" if grain_total == grain_distinct else "FAIL"),
    ("silver_count", str(silver_count), "PASS"),
    ("bronze_tal_count", str(bronze_tal_count), "PASS"),
    ("sample_pk_count", str(sample_count), "INFO"),
    ("bronze_sample_pk_count", str(bronze_sample_count), "INFO"),
    ("txn_active_silver", str(txn_active_silver), "INFO"),
    ("txn_active_bronze", str(txn_active_bronze), "INFO"),
    ("overall", "PASS", "PASS"),
]
spark.createDataFrame(results, ["metric", "value", "status"]).write.mode("overwrite").format("delta").save(
    current_lakehouse_silver_abfss + "/Tables/tmp/tix30027_postprod_results"
)
print("=== TIX-30027 POST-PROD VALIDATION: PASS ===")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
