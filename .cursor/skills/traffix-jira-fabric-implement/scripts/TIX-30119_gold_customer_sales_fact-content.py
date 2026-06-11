# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# ## Spark Configuration

# CELL ********************

%run spark_configuration

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Splunk Configuration

# CELL ********************

%run splunk_configuration

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# splunk log type for this job
splunk_log_type='ERROR'

# define splunk function to submit logs
def call_splunk(payload):
    splunkIndex=splunk_index
    splunkLoggingUri = 'https://http-inputs-traffix.splunkcloud.com/services/collector/event'
    splunkHeader = {"Authorization": f"Splunk {splunkIndex}"}
    req = requests.post(splunkLoggingUri, headers=splunkHeader,json=payload)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Gold Configuraton

# CELL ********************

# table path

path_source_tgtms_hub="/Tables/tgtms_hub/"
path_source_netsuite="/Tables/netsuite/"
path_source_dimensions="/Tables/dimensions/"
path_source_dimension="/Tables/dimension/"
path_source_facts="/Tables/customer_sales/"
path_source_finance="/Tables/finance/"
path_source_lookup = "/Tables/lookup/"
path_target="/Tables/customer_sales/"

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

# global full load setting
print("global full load:", global_full_load)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#TimeDelta for Late Arriving Data
print("timedelta_adjustment:", timedelta_adjustment) 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## History Start Date

# CELL ********************

history_start_date='2024-01-01'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# 
# ## Declare target table properties & detect load mode
# 
# Initializes target/source table names and flags, then attempts to read the target table from the Gold lakehouse to determine whether an **incremental** or **full** load should run.
# 
# - **Sets defaults**: `table_target_exists=False`, `table_target_abfss=None`, `table_target_full_load=False`
# - **Defines names**: `table_target="customer_sales_fact"`, `table_source="customer_sales_fact_source"`
# - **Reads target**: `spark.read.load(current_lakehouse_gold_abfss + path_target + table_target)`
# - **Load mode**:
#   - If read succeeds → marks `table_target_exists=True` and prints _“incremental load will start”_
#   - If read fails (e.g., path missing on first run) → prints _“full load will start”_ with the exception
# - **Key columns**: `primary_key="primary_key"`, `date_modified="__sys_synced"`


# CELL ********************

# declare target table properties

table_target_exists=False
table_target_abfss=None
table_target_full_load=False

try:
    # notebookutils.fs.ls(current_lakehouse_silver_abfss+path_target+table_target)
    table_target = "fact_customer_sales"
    table_source =f"{table_target}"+"_source"
    table_target_abfss=spark.read.load(current_lakehouse_gold_abfss+path_target+table_target)
    primary_key="primary_key"
    date_modified="__sys_synced"
    table_target_exists=True
    print(f"{table_target} incremental load will start")
except Exception as e: # can return Path does not exist error if it is first full load
    print(f"{table_target} full load will start since: ",e)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# 
# ## Load source tables from Fabric lakehouses
# 
# Reads multiple Delta tables from **Silver**, **Bronze**, and **Gold** layers into Spark DataFrames for downstream transformations.
# 
# - **Shipment & Order Data**: `shipment_load_df`, `shipment_order_leg_df`, `order_leg_df`
# - **Reference & Dimensions**: `organization_df`, `silver_load_reference_df`, `gold_user_df`, `branch_df`, `equipment_df`
# - **Financial Data**: `currency_df`, `mrt_ar_df`, `mrt_invoice_df`, `invoice_ord_header_df`
# - **Additional Entities**: `silver_load_df`, `silver_load_job_function_df`, `df_order_line`, `silver_load_allocation_df`
# - **Format**: All tables are read in **Delta** format using `spark.read.format("delta").load(...)`
# ``


# CELL ********************

shipment_load_df = spark.read.format("delta").load(production_lakehouse_silver_abfss+path_source_tgtms_hub+'shipment_load').alias("s1")
shipment_order_leg_df = spark.read.format("delta").load(production_lakehouse_silver_abfss+path_source_tgtms_hub+'shipment_order_leg').alias("s2")
order_leg_df = spark.read.format("delta").load(production_lakehouse_silver_abfss+path_source_tgtms_hub+'order_leg').alias("o1")
organization_df = spark.read.format("delta").load(production_lakehouse_silver_abfss+path_source_tgtms_hub+'organization').alias("o3")
silver_load_df = spark.read.format("delta").load(production_lakehouse_silver_abfss+path_source_tgtms_hub+'load').alias("load")
currency_df = spark.read.format("delta").load(production_lakehouse_bronze_abfss+path_source_tgtms_hub+'CurrencyExchangeRate').alias("c1")
silver_load_reference_df  = spark.read.format("delta").load(production_lakehouse_silver_abfss+path_source_tgtms_hub+'load_reference_number').alias("lrn")
gold_user_df = spark.read.format("delta").load(production_lakehouse_gold_abfss+path_source_dimensions+'user').alias("dim")
silver_load_job_function_df  = spark.read.format("delta").load(production_lakehouse_silver_abfss + path_source_tgtms_hub + 'load_job_function')
df_order_line = spark.read.format("delta").load(production_lakehouse_silver_abfss + path_source_tgtms_hub + 'order_line')
branch_df  = spark.read.format("delta").load(production_lakehouse_gold_abfss + path_source_dimensions + 'branch').alias("branch")
equipment_df = spark.read.format("delta").load(production_lakehouse_gold_abfss + path_source_dimensions + "equipment").alias("e1")
silver_load_allocation_df = spark.read.format("delta").load(production_lakehouse_silver_abfss+path_source_tgtms_hub+'load_allocation').alias("lrn")
mrt_ar_df = spark.read.format("delta").load(production_lakehouse_silver_abfss + path_source_tgtms_hub + 'mrt_ar').alias("ar")
mrt_invoice_df = spark.read.format("delta").load(production_lakehouse_silver_abfss + path_source_tgtms_hub + 'mrt_invoice').alias("inv")
invoice_ord_header_df = spark.read.format("delta").load(production_lakehouse_silver_abfss + path_source_tgtms_hub + 'invoice_ord_header').alias("ioh")
df_freight_bill  = spark.read.format("delta").load(production_lakehouse_silver_abfss + path_source_tgtms_hub + 'freight_bill').alias("fb")
df_load_cost  = spark.read.format("delta").load(production_lakehouse_silver_abfss + path_source_tgtms_hub + 'load_cost').alias("lc")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# 
# ## Build order–load base and enrich with delivery dates
# 
# Determines **load type** (full vs incremental), assembles the core order–load dataset, and enriches it with the latest delivery date per order.
# 
# - **Load type**:
#   - Sets `load_type` to `"full"` if target table doesn’t exist or any full-load flag is true; otherwise `"incremental"`.
# 
# - **Base dataset (`df_order_load`)**:
#   - Joins:
#     - `shipment_load_df (s1)` ⟷ `shipment_order_leg_df (s2)` on `shipment_id` where `s2.__sys_deleted = 0`
#     - `shipment_order_leg_df (s2)` ⟷ `order_leg_df (o1)` on `order_leg_id` where `o1.__sys_deleted = 0`
#     - `organization_df (o3)` on `o3.organization_id = s1.organization_id`
#   - Filters:
#     - `o3.organization_parent_id = 4` (scope to parent org)
#     - `s1.created_date >= '2023-01-01'` (date floor)
#     - `o1.order_status IS NOT NULL`
#     - `s1.__sys_deleted = 0`
#   - Selects: `load_id`, `order_header_id`, client info, sync markers (`__sys_synced`), and deduplicates.
#   - Orders by `order_header_id`, `load_id` (for deterministic output).
# 
# - **Load slim (`load_slim_df`)**:
#   - Reads `silver_load_df` and keeps `load_id`, `delivery_date`, `delivery_date_type`, excluding deleted (`__sys_deleted = 0`).
# 
# - **Attach delivery to loads**:
#   - `order_load_with_dates = df_order_load ⟷ load_slim_df` on `load_id` (left join) to bring per-load delivery metadata.
# 
# - **Compute per-order delivery summary**:
#   - `max_delivery_per_order`: groups by `order_header_id` and derives:
#     - `order_delivery_date = to_date(max(delivery_date))` (latest delivery)
#     - `order_delivery_date_type = max_by(delivery_date_type, delivery_date)` (type associated with latest date)
# 
# - **Final enriched output**:


# CELL ********************

from pyspark.sql import functions as F

# -----------------------------
# Parameters / target path
# -----------------------------
gold_table_path = f"{current_lakehouse_gold_abfss+path_target+table_target}"

# -----------------------------
# Determine load type
# -----------------------------
if (not table_target_exists) or table_target_full_load or global_full_load:
    load_type = "full"
else:
    load_type = "incremental"


df_order_load = (
    shipment_load_df.join(shipment_order_leg_df, (F.col("s2.shipment_id") == F.col("s1.shipment_id")) & (F.col("s2.__sys_deleted") == F.lit(0)), "inner")
      .join(order_leg_df, (F.col("o1.order_leg_id") == F.col("s2.order_leg_id")) & (F.col("o1.__sys_deleted") == F.lit(0)), "inner")
      .join(organization_df, F.col("o3.organization_id") == F.col("s1.organization_id"), "inner")
      .where(
          (F.col("o3.organization_parent_id") == F.lit(4)) &
          (F.col("s1.created_date") >= F.to_date(F.lit("2023-01-01"))) &
          (F.col("o1.order_status").isNotNull()) &
          (F.col("s1.__sys_deleted") == F.lit(0))
      )
      .selectExpr(
          "s1.load_id",
          "o1.order_header_id",
          "o1.trading_partner_client_name",
          "o1.trading_partner_client_number",
          "s1.__sys_synced",
          "s1.__sys_deleted",
          "s2.__sys_synced AS s2__sys_synced",
          "o1.__sys_synced AS o1__sys_synced"
      )
      .distinct()
      .orderBy(F.col("o1.order_header_id"), F.col("s1.load_id"))
)

# silver_load_df = spark.read.format("delta").load(... + 'load').alias("load")

load_slim_df = (
    silver_load_df
        .select("load_id", "delivery_date", "delivery_date_type", "__sys_deleted")
        .where(F.col("__sys_deleted") == F.lit(0))
)

order_load_with_dates = df_order_load.join(load_slim_df, on="load_id", how="left")

max_delivery_per_order = (
    order_load_with_dates
        .groupBy("order_header_id")
        .agg(
            F.to_date(F.max("delivery_date")).alias("order_delivery_date"),
            F.max_by(F.col("delivery_date_type"), F.col("delivery_date")).alias("order_delivery_date_type")
        )
)



# Enrich original
df_order_load_enriched = df_order_load.join(max_delivery_per_order, on="order_header_id", how="left")



# 1849229

#  filtered_df = df_order_load_enriched.where(F.col("load_id") == 1837299)

# display(filtered_df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# 
# ## Currency join, intracompany load filter, and user dimension prep
# 
# Builds currency conversion lookups, filters **intracompany** loads with required reference numbers, and prepares a clean **User** dimension.
# 
# - **Currency (USD → CAD/MXN rates)**:
#   - Creates `mxn_df` by self-joining `CurrencyExchangeRate` on matching `Date_Effective`.
#   - Keeps rows where **base currency = USD (`CurrencyId=1`)** and **operating currency = MXN (`CurrencyId=3`)**, with `DateCreated ≥ 2023-01-01`.
#   - Outputs:
#     - `exchange_rate_id_base_to_operating` = `c1.CurrencyExchangeRateId`
#     - `exchange_rate_cad_to_mxn` = `c2.ExchangeRateInverse`
#     - `exchange_rate_cad_to_usd` = `c1.ExchangeRateInverse`
# 
# - **Reference numbers pre-filter**:
#   - `l2`: logistics load number (`qualifier_id=110`), not deleted → selects `load_id`, `reference_number_value`.
#   - `l3`: intracompany flag (`qualifier_id=112`), not deleted → selects `load_id`.
#   - Predicate pushdown reduces shuffle before joins.
# 
# - **Intracompany loads (`ltc_df`)**:
#   - From `silver_load_df (l1)`:
#     - Filters: `organization_id=7`, `created_date ≥ 2023-01-01`, `load_status != 'Canceled'`, `__sys_deleted=0`.
#   - Inner joins to `l2` and `l3` on `load_id` to ensure required references exist.
#   - Selects core fields: `load_id`, `ltl_load_number`, net costs (currency, operating, base), `currency_code`, `logistics_load_number`, carrier info, `intracompany_load=1`, `tender_date`, `distance`.
#   - Deduplicates with `.distinct()`.
# 
# - **User dimension (`user_df`)**:
#   - From `gold_user_df`:
#     - Filters: `__sys_deleted=0`, email contains `traffix.com`, and `home_currency` is defined.
#   - Selects: `user_key`, `user_id`, `email`, `home_currency`.
#   - Deduplicates with `.distinct()`.


# CELL ********************

# Currency
# Alias for clarity
c1 = currency_df.alias("c1")
c2 = currency_df.alias("c2")

mxn_df = (
    c1.join(
        c2,
        (F.col("c2.Date_Effective") == F.col("c1.Date_Effective")) &
        (F.col("c2.CurrencyId") == F.lit(3)),  # MXN
        "inner"
    )
    .where(
        (F.col("c1.CurrencyId") == F.lit(1)) &  # USD
        (F.col("c1.DateCreated") >= F.to_date(F.lit("2023-01-01")))
    )
    .select(
        F.col("c1.CurrencyExchangeRateId").alias("exchange_rate_id_base_to_operating"),
        F.col("c2.ExchangeRateInverse").alias("exchange_rate_cad_to_mxn"),
        F.col("c1.ExchangeRateInverse").alias("exchange_rate_cad_to_usd")  # <— add this
    )
)

# Aliases for clarity
l1 = silver_load_df.alias("l1")
lref = silver_load_reference_df

# Pre-filter reference numbers to push down predicates and reduce shuffle
l2 = (
    lref.alias("l2")
        .where((F.col("l2.__sys_deleted") == F.lit(0)) & (F.col("l2.qualifier_id") == F.lit(110)))  # logistics load number
        .select("l2.load_id", "l2.reference_number_value")
)

l3 = (
    lref.alias("l3")
        .where((F.col("l3.__sys_deleted") == F.lit(0)) & (F.col("l3.qualifier_id") == F.lit(112)))  # intracompany Y
        .select("l3.load_id")
)


ltc_df = (
    l1
    .where(
        (F.col("l1.organization_id") == F.lit(7)) &
        (F.col("l1.created_date") >= F.to_date(F.lit("2023-01-01"))) &
        (F.col("l1.load_status") != F.lit("Canceled")) &
        (F.col("l1.__sys_deleted") == F.lit(0))
    )
    .join(l2, on=F.col("l1.load_id") == F.col("l2.load_id"), how="inner")
    .join(l3, on=F.col("l1.load_id") == F.col("l3.load_id"), how="inner")
    .select(
        F.col("l1.load_id"),
        F.col("l1.load_number").alias("ltl_load_number"),
        F.col("l1.currency_net_cost"),
        F.col("l1.currency_operating_net_cost"),
        F.col("l1.currency_base_net_cost"),
        F.col("l1.currency_code"),
        F.col("l2.reference_number_value").alias("logistics_load_number"),
        F.col("l1.trading_partner_carrier_number"),
        F.col("l1.trading_partner_carrier_name"),
        F.lit(1).alias("intracompany_load"),
        F.col("l1.tender_date"),
        F.col("l1.distance")
    )
    .distinct()
)

#User 
user_df = (
    gold_user_df
        .where(
            (F.col("__sys_deleted") == F.lit(0)) &
            (F.col("email").like("%traffix.com%")) &
            (F.col("home_currency") != F.lit("undefined"))
        )
        .select(
            F.col("user_key"),
            F.col("user_id"),
            F.col("email"),
            F.col("home_currency")
        )
        .distinct()
)




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# 
# ## Sales rep assignment from Job Function (id=72)
# 
# Pre-filters **Load Job Function** records to extract the sales rep (username) tied to loads, then unions sources and deduplicates.
# 
# - **Scope & filters**:
#   - `job_function_id = 72` (Sales Rep)
#   - `created_date ≥ 2023-01-01`
#   - `__sys_deleted = 0`
#   - For `jf_order_df`, also requires non-null `load_id`
# 
# - **Sources**:
#   - `jf_load_df`: from `silver_load_job_function_df (ljf)` selecting `load_id`, `username → salesrep_id`
#   - `jf_order_df`: from `silver_load_job_function_df (ojf)` selecting `load_id`, `username → salesrep_id`
# 
# - **Combine & dedupe**:
#   - `jf_df = jf_load_df UNION DISTINCT jf_order_df` to merge both paths and remove duplicates.


# CELL ********************

# Job Function
# Pre-filter each source to push down predicates and minimize shuffle
jf_load_df = (
    silver_load_job_function_df.alias("ljf")
        .where(
            (F.col("ljf.job_function_id") == F.lit(72)) &
            (F.col("ljf.created_date") >= F.to_date(F.lit("2023-01-01"))) &
            (F.col("ljf.__sys_deleted") == F.lit(0))
        )
        .select(
            F.col("ljf.load_id"),
            F.col("ljf.username").alias("salesrep_id")
        )
        .distinct()
)

jf_order_df = (
    silver_load_job_function_df.alias("ojf")
        .where(
            (F.col("ojf.job_function_id") == F.lit(72)) &
            (F.col("ojf.created_date") >= F.to_date(F.lit("2023-01-01"))) &
            (F.col("ojf.load_id").isNotNull()) &
            (F.col("ojf.__sys_deleted") == F.lit(0))
        )
        .select(
            F.col("ojf.load_id"),
            F.col("ojf.username").alias("salesrep_id")
        )
        .distinct()
)

# UNION DISTINCT across both sources
jf_df = jf_load_df.unionByName(jf_order_df, allowMissingColumns=False).distinct()




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# 
# ## Assign default Sales Rep per load and enrich order–load rows
# 
# Determines a **single, deterministic sales rep** for each load and attaches it to the order–load dataset.
# 
# - **(A) One sales rep per load**:
#   - `jf_single_df`: groups `jf_df` by `load_id` and picks `MIN(salesrep_id)` for repeatable selection.
# 
# - **(B) Load creator fallback**:
#   - `load_creator_df`: extracts `created_by_username` from `silver_load_df`.
# 
# - **(C) Defaulting logic (priority order)**:
#   - `salesrep_id_default = COALESCE(jf.salesrep_id, created_by_username, 'unassigned')`
#     - Primary: Job Function assignment
#     - Fallback #1: Load creator
#     - Fallback #2: constant `'unassigned'`
# 
# - **(D) Enrichment**:
#   - `df_order_load_with_salesrep_default`: joins `load_salesrep_default_df` to `df_order_load` on `load_id` to add `salesrep_id_default` to each order–load record.


# CELL ********************

#Load with SalesRep

# --- (A) Keep one salesrep per load deterministically from jf_df ---
jf_single_df = (
    jf_df
      .groupBy("load_id")
      .agg(F.min("salesrep_id").alias("salesrep_id"))  # deterministic selection
)

# --- (B) Bring in created_by_username from the load table ---
# silver_load_df already loaded (path-based)
load_creator_df = (
    silver_load_df
      .select("load_id", "created_by_username")
)

# --- (C) Build defaulted salesrep per load (JF -> created_by -> constant) ---
load_salesrep_default_df = (
    load_creator_df
      .join(jf_single_df, on="load_id", how="left")
      .withColumn(
          "salesrep_id_default",
          F.coalesce(
              F.col("salesrep_id"),         # primary: job function assignment
              F.col("created_by_username"), # fallback #1: who created the load
              F.lit("unassigned")           # fallback #2: constant default (customize as needed)
          )
      )
      .select("load_id", "salesrep_id_default")
)

# --- (D) Enrich df_order_load with the defaulted sales rep ---
df_order_load_with_salesrep_default = (
    df_order_load
      .join(load_salesrep_default_df, on="load_id", how="left")
)




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# 
# ## Determine primary Sales Rep per order (JF-first, creator fallback)
# 
# Computes an **order-level** default sales rep using job-function assignments across loads, with a fallback to the most frequent load creator.
# 
# - **Base source (`order_rep_source`)**:
#   - Joins `df_order_load` with `jf_df` (rep_from_jf) and `silver_load_df` (created_by_username).
#   - Fields: `order_header_id`, `load_id`, `rep_from_jf`, `created_by_username`.
# 
# - **Primary rule (Job Function)**:
#   - `order_jf_counts`: for each `(order_header_id, rep_from_jf)`, counts **distinct loads**.
#   - Window `w_jf`: rank reps per order by `load_count_for_rep DESC`, then `rep_from_jf ASC` (tie-break).
#   - `order_primary_rep_from_jf`: picks `row_number = 1` → `primary_salesrep_id` and `primary_rep_loads`.
# 
# - **Fallback rule (Creator)**:
#   - `order_created_counts`: for each `(order_header_id, created_by_username)`, counts **distinct loads**.


# CELL ********************

#Order with SalesRep
from pyspark.sql import functions as F, Window

# Base source tying loads, jf assignments, and created_by user
order_rep_source = (
    df_order_load
      .join(jf_df, on="load_id", how="left")
      .join(silver_load_df.select("load_id", "created_by_username"), on="load_id", how="left")
      .select(
          "order_header_id",
          "load_id",
          F.col("salesrep_id").alias("rep_from_jf"),
          F.col("created_by_username")
      )
)

# Primary: counts of JF reps per order
order_jf_counts = (
    order_rep_source
      .where(F.col("rep_from_jf").isNotNull())
      .groupBy("order_header_id", "rep_from_jf")
      .agg(F.countDistinct("load_id").alias("load_count_for_rep"))
)

w_jf = Window.partitionBy("order_header_id").orderBy(F.col("load_count_for_rep").desc(), F.col("rep_from_jf").asc())

order_primary_rep_from_jf = (
    order_jf_counts
      .withColumn("rn", F.row_number().over(w_jf))
      .where(F.col("rn") == 1)
      .select(
          "order_header_id",
          F.col("rep_from_jf").alias("primary_salesrep_id"),
          F.col("load_count_for_rep").alias("primary_rep_loads")
      )
)

# Fallback: most frequent created_by_username per order
order_created_counts = (
    order_rep_source
      .where(F.col("created_by_username").isNotNull())
      .groupBy("order_header_id", "created_by_username")
      .agg(F.countDistinct("load_id").alias("load_count_for_creator"))
)

w_created = Window.partitionBy("order_header_id").orderBy(F.col("load_count_for_creator").desc(), F.col("created_by_username").asc())

order_primary_creator = (
    order_created_counts
      .withColumn("rn", F.row_number().over(w_created))
      .where(F.col("rn") == 1)
      .select(
          "order_header_id",
          F.col("created_by_username").alias("fallback_salesrep_id")
      )
)

# Merge to produce the defaulted order-level sales rep
order_salesrep_default = (
    order_primary_rep_from_jf
      .join(order_primary_creator, on="order_header_id", how="left")
      .withColumn(
          "salesrep_id_default",
          F.coalesce(F.col("primary_salesrep_id"), F.col("fallback_salesrep_id"), F.lit("unassigned"))
      )
      .select("order_header_id", "salesrep_id_default")
)

# Enrich df_order_load
df_order_load_with_order_salesrep_default = (
    df_order_load
      .join(order_salesrep_default, on="order_header_id", how="left")
)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# 
# ## Sales rep & customer counts per load + canonical load–customer pairs
# 
# Computes metrics per **load** and prepares a normalized list of **(load, customer)** pairs.
# 
# - **Sales Rep Count (`salesrep_count_df`)**:
#   - For each `load_id`, counts **distinct** `salesrep_id` from `jf_df` where `salesrep_id IS NOT NULL`.
#   - Output: `load_id`, `salesrep_count`.
# 
# - **Customer Count (`customer_count_df`)**:
#   - For each `load_id`, counts **distinct** `trading_partner_client_number` from `df_order_load` where it is not null.
#   - Output: `load_id`, `customer_count`.
# 
# - **Normalize column names**:
#   - Renames any columns with dots (e.g., `o1.trading_partner_client_number`) by replacing `.` with `_` to avoid Spark column path issues.
# 
# - **Canonical fields**:
#   - Uses `customer_key_col = trading_partner_client_number` and `customer_name_col = trading_partner_client_name` for consistent downstream logic.
# 
# - **Load–Customer Pairs (`load_customer_pairs_df`)**:
#   - From `df_order_load`, selects unique combinations of `load_id`, `customer_number`, and `customer_name` where the customer key is present.
#   - Ensures each `(load_id, customer)` is listed once via `.distinct()`.


# CELL ********************

#Sales Rep Count 
#Customer Count
# Distinct sales reps per load_id from jf_df
salesrep_count_df = (
    jf_df
      .where(F.col("salesrep_id").isNotNull())
      .groupBy("load_id")
      .agg(F.countDistinct("salesrep_id").alias("salesrep_count"))
)

customer_count_df = (
    df_order_load
      .where(F.col("trading_partner_client_number").isNotNull())
      .groupBy("load_id")
      .agg(F.countDistinct("trading_partner_client_number").alias("customer_count"))
)

# Normalize dotted column names if any exist (e.g., 'o1.trading_partner_client_number')
for c in df_order_load.columns:
    if '.' in c:
        df_order_load = df_order_load.withColumnRenamed(c, c.replace('.', '_'))

# Choose canonical columns (adjust if your df_order_load uses different names)
customer_key_col = "trading_partner_client_number"
customer_name_col = "trading_partner_client_name"

load_customer_pairs_df = (
    df_order_load
      .where(F.col(customer_key_col).isNotNull())
      .select("load_id", F.col(customer_key_col).alias("customer_number"), F.col(customer_name_col).alias("customer_name"))
      .distinct()
)






# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# 
# ## Compute total handling unit length per load (3-decimal ceiling)
# 
# Joins **order–load** rows to **order_line** to aggregate handling unit lengths by load and produce a ceiling-rounded metric.
# 
# - **Join**:
#   - `df_order_load (o1)` ⟷ `df_order_line (o2)` on `order_header_id`
#   - Filters: `o1.__sys_deleted = 0` and `o2.__sys_deleted = 0`
# 
# - **Aggregation**:
#   - `sum_len = SUM(o2.handling_unit_length_ft)`; `COALESCE` to `0.0` if null
#   - `total_length = CEIL(sum_len * 1000) / 1000` → ceilings to **3 decimal places**
# 
# - **Output**:


# CELL ********************

# Aliases
o1 = df_order_load.alias("o1")
o2 = df_order_line.alias("o2")  # Ensure this DataFrame is already loaded as your Silver 'order_line'

# Aggregation: sum handling_unit_length_ft per load_id, ceiling to 3 decimals
sum_len = F.coalesce(F.sum(F.col("o2.handling_unit_length_ft")), F.lit(0.0))
total_length_ceil_3 = (F.ceil(sum_len * F.lit(1000))) / F.lit(1000)

df_order_length = (
    o1.join(
        o2,
        (F.col("o2.order_header_id") == F.col("o1.order_header_id")) &
        (F.col("o2.__sys_deleted") == F.lit(0)),
        "inner"
    )
    .where(F.col("o1.__sys_deleted") == F.lit(0))   # ✅ corrected column name
    .groupBy(F.col("o1.load_id"))
    .agg(total_length_ceil_3.alias("total_length"))
)




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# 
# ## Finalize order–load record with sales rep, delivery, length, and best invoice
# 
# Builds the consolidated **order–load** dataset by attaching sales rep defaults (load & order), delivery dates (load & order), aggregated length, and the **best** invoice per order.
# 
# - **Sales Rep defaults (renamed)**:
#   - `load_salesrep_id_default`: from load-level default (`COALESCE(jf → created_by → 'unassigned')`)
#   - `order_salesrep_id_default`: from order-level default (JF-most-loads → creator-most-loads → `'unassigned'`)
#   - Renamed before joining to avoid column collisions.
# 
# - **Invoice selection (priority window)**:
#   - `status_priority`: `Paid=1`, `PaidPartial=2`, `Billed=3`, else `9`
#   - `w_best_inv`: ranks invoices per `order_header_id` by:
#     1) **status_priority ASC**,
#     2) `last_modified_date DESC NULLS LAST`,
#     3) `created_date DESC NULLS LAST`
#   - `invoice_latest_per_order_df`: picks `rn=1` (best invoice) and keeps header fields.
#   - ⚠️ Note: `w_latest_inv_per_order` is defined (latest timestamps + invoice_number) but **not used**. If you intended “latest invoice” rather than “best status,” swap to that window.
# 
# - **Joins**:
#   - `df_order_load` ⟷ `load_salesrep_default_renamed` on `load_id`
#   - `df_order_load` ⟷ `order_salesrep_default_renamed` on `order_header_id`
#   - `df_order_load` ⟷ `load_slim_df` on `load_id` (load delivery date/type)
#   - `df_order_load` ⟷ `df_order_load_enriched` on `order_header_id` (order-level delivery summary)
#   - `df_order_load` ⟷ `df_order_length` on `load_id` (aggregated handling-unit length)
#   - `df_order_load` ⟷ **broadcast**(`invoice_latest_per_order_df`) on `order_header_id` (best invoice header)
# 
# - **Selected output fields**:
#   - **Lineage/CDC**: `load_id`, `order_header_id`, `__sys_synced`, `__sys_deleted`
#   - **Customer**: `trading_partner_client_name`, `trading_partner_client_number`
#   - **Sales Rep**: `load_salesrep_id_default`, `order_salesrep_id_default`
#   - **Delivery**: `load_delivery_date`, `load_delivery_date_type`, `order_delivery_date`, `order_delivery_date_type`
#   - **Metrics**: `total_length`
#   - **Invoice header**: `invoice_id`, `invoice_number`, `invoice_status`, `invoice_type`, `invoice_last_modified_date`, `invoice_created_date`


# CELL ********************

#Final OrderLoad Dataframe
from pyspark.sql import functions as F
# 1) Rename salesrep columns BEFORE joining
load_salesrep_default_renamed = (
    load_salesrep_default_df
      .select(
          "load_id",
          F.col("salesrep_id_default").alias("load_salesrep_id_default")
      )
)

order_salesrep_default_renamed = (
    order_salesrep_default
      .select(
          "order_header_id",
          F.col("salesrep_id_default").alias("order_salesrep_id_default")
      )
)

# ------------------------------------------------------------
# 2. Status priority
# ------------------------------------------------------------
status_priority = (
    F.when(F.col("invoice_status") == "Paid", 1)
     .when(F.col("invoice_status") == "PaidPartial", 2)
     .when(F.col("invoice_status") == "Billed", 3)
     .otherwise(9)
)

# Toggle: include Manifest as revenue
include_manifest = False

# ------------------------------------------------------------
# 3. Invoice selection (Revenue Hierarchy)
#    Rebill > Original > Manifest ; exclude CreditMemo/NULL
#    Tie-breakers: status > last_modified_date > created_date > invoice_number
# ------------------------------------------------------------
invoices_norm = (
    invoice_ord_header_df
      .where(F.col("__sys_deleted") == F.lit(0))
      .withColumn("invoice_type_norm", F.upper(F.coalesce(F.col("invoice_type"), F.lit("NULL"))))
)

valid_for_revenue = invoices_norm.where(~F.col("invoice_type_norm").isin("CREDITMEMO", "NULL"))
if not include_manifest:
    valid_for_revenue = valid_for_revenue.where(F.col("invoice_type_norm") != "MANIFEST")

valid_for_revenue = valid_for_revenue.withColumn(
    "type_priority",
    F.when(F.col("invoice_type_norm") == "REBILL", 1)
     .when(F.col("invoice_type_norm") == "ORIGINAL", 2)
     .when(F.col("invoice_type_norm") == "MANIFEST", 3)
     .when(F.col("invoice_type_norm") == "SUPPLEMENTAL", 99)  # do not select as primary
     .otherwise(999)
)

# IMPORTANT: use .desc() instead of .desc_nulls_last() for broad compatibility
w_primary = Window.partitionBy("order_header_id").orderBy(
    F.col("type_priority").asc(),
    status_priority.asc(),
    F.col("last_modified_date").desc(),
    F.col("created_date").desc(),
    F.col("invoice_number").desc()
)

invoice_primary_per_order_df = (
    valid_for_revenue
      .withColumn("rn", F.row_number().over(w_primary))
      .where(F.col("rn") == 1)
      .select(
          F.col("order_header_id"),
          F.col("invoice_id"),
          F.col("invoice_number"),
          F.col("invoice_status"),
          F.col("invoice_type").alias("invoice_type"),
          F.col("last_modified_date").alias("invoice_last_modified_date"),
          F.col("created_date").alias("invoice_created_date")
      )
      .alias("ioh_primary")
)

# Optional: collect Supplemental separately
invoice_supplemental_df = (
    invoices_norm
      .where((F.col("__sys_deleted") == 0) & (F.col("invoice_type_norm") == "SUPPLEMENTAL"))
      .select(
          "order_header_id", "invoice_id", "invoice_number", "invoice_status", "invoice_type",
          "last_modified_date", "created_date"
      )
)

# ------------------------------------------------------------
# 4. Final Order–Load dataset join
# ------------------------------------------------------------
df_order_load_final = (
    df_order_load.alias("dfload")
      .join(load_slim_df.alias("lslim"), on="load_id", how="left")
      .join(df_order_load_enriched.alias("orde"), on="order_header_id", how="left")
      .join(df_order_length.alias("ol"), on="load_id", how="left")
      .join(F.broadcast(invoice_primary_per_order_df).alias("ioh"), on="order_header_id", how="left")
      .select(
        F.col("dfload.load_id").alias("load_id"),
        F.col("dfload.order_header_id").alias("order_header_id"),
        F.col("dfload.trading_partner_client_name").alias("trading_partner_client_name"),
        F.col("dfload.trading_partner_client_number").alias("trading_partner_client_number"),
        F.to_date(F.col("lslim.delivery_date")).alias("load_delivery_date"),
        F.col("lslim.delivery_date_type").alias("load_delivery_date_type"),
        F.col("orde.order_delivery_date").alias("order_delivery_date"),
        F.col("orde.order_delivery_date_type").alias("order_delivery_date_type"),
        F.col("ol.total_length").alias("total_length"),
        F.col("dfload.__sys_synced").alias("__sys_synced"),
        F.col("dfload.__sys_deleted").alias("__sys_deleted"),
        F.col("ioh.invoice_id").alias("invoice_id"),
        F.col("ioh.invoice_number").alias("invoice_number"),
        F.col("ioh.invoice_status").alias("invoice_status"),
        F.col("ioh.invoice_type").alias("invoice_type"),
        F.col("ioh.invoice_last_modified_date").alias("invoice_last_modified_date"),
        F.col("ioh.invoice_created_date").alias("invoice_created_date")
      )
)





# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# 
# ## Define aliases for downstream joins (clarity & consistency)
# 
# Establishes short, readable aliases for frequently used DataFrames from **Silver/Gold** layers and previously built intermediates. This improves join readability and reduces mistakes in column qualification.
# 
# - **Core Silver/Gold sources**:
#   - `l1`: `silver_load_df` → `[LH_Silver].[tgtms_hub].[load]`
#   - `l2`: `silver_load_allocation_df` → `[LH_Silver].[tgtms_hub].[load_allocation]`
#   - `inv`: `mrt_invoice_df` → invoice facts `[LH_Silver].[tgtms_hub].[mrt_invoice]`
#   - `b1`, `b2`: `branch_df` (Gold dimension) for multi-branch joins
# 
# - **Prepared/intermediate frames**:
#   - `ltc`: `ltc_df` → intracompany loads (filtered with qualifiers 110/112)
#   - `jf`: `jf_df` → job-function derived sales reps per load
#   - `jfc`: `salesrep_count_df` → distinct sales rep count per load
#   - `d1`: `df_order_length` → total handling unit length per load (3-decimal ceiling)
#   - `ci`: `load_customer_pairs_df` → canonical `(load_id, customer_number, customer_name)`
#   - `cc`: `customer_count_df` → distinct customer count per load
#   - `us`: `user_df` → users with `home_currency` and `traffix.com` emails
#   - `mxn`: `mxn_df` → currency lookup with `exchange_rate_cad_to_mxn`, `exchange_rate_cad_to_usd`
#   - `load_order`: `df_order_load_final` → consolidated order–load record with sales reps, delivery, length, invoice header
# 
# > Use these aliases in subsequent joins to minimize verbose column paths and keep transformations readable (e.g., `l1.join(jf, "load_id")`).


# CELL ********************

# ALIAS-DF Mapping
# Silver (Delta)
l1 = silver_load_df.alias("l1")  # [LH_Silver].[tgtms_hub].[load]
l2 = silver_load_allocation_df.alias("l2")  # [LH_Silver].[tgtms_hub].[load_allocation]
inv = mrt_invoice_df.alias("i")
ltc = ltc_df.alias("ltc")  # built earlier
jf  = jf_df.alias("jf")    # built earlier (salesrep_id per load)
jfc = salesrep_count_df.alias("jfc")
load_order = df_order_load_final.alias("ordl")

# Other prepared DataFrames
us  = user_df.alias("us")               # built earlier (user_key, user_id, home_currency)
mxn = mxn_df.alias("mxn")             # built earlier (exchange_rate_id_base_to_operating → exchange_rate_cad_to_mxn)
d1  = df_order_length.alias("d1")     # built earlier (total_length per load_id)
ci  = load_customer_pairs_df.alias("ci")               # should have columns: load_id, customer_key
cc  = customer_count_df.alias("cc")   # per-load customer_count
b1  = branch_df.alias("b1")
b2  = branch_df.alias("b2")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# 
# ## Build base load fact, attach dims & measures, then normalize/split by Sales Rep
# 
# Creates the **base load-level fact** by joining core tables, projecting keys/measures, and then normalizing **Revenue/Cost** to the sales rep’s **home currency** with a per‑rep split.
# 
# ### Joins & Filters
# - **Joins**:
#   - Organization (`organization_df` on `l1.organization_id`) — *note: uses `o3.sys_deleted=0`; ensure column name is correct (`__sys_deleted` elsewhere).*
#   - Load Allocation `l2` (AR/Revenue)
#   - Branch dimensions `b1` (primary) and `b2` (alternative)
#   - Intracompany/LTL `ltc` (qualifiers 110/112)
#   - Sales Rep assignments `jf` and counts `jfc`
#   - Customer identity & counts: `ci` (pairs), `cc` (per-load count)
#   - User `us` (match `email = jf.salesrep_id`) for `home_currency` and `user_key`
#   - Exchange rates `mxn` via `exchange_rate_id_base_to_operating`
#   - Total length `d1`
#   - Consolidated order/invoice/delivery `load_order`
# - **Filters**:
#   - `l1.created_date >= '2023-01-01'`
#   - `o3.organization_parent_id = 4`
#   - `l1.__sys_deleted = 0`
# 
# ### Projection (selected columns)
# - **Primary key**: `primary_key = concat_ws("-", load_id, coalesce(user_key, "undefined"), coalesce(upper(customer_number), "undefined"))`
# - **Keys & status**: `load_id`, `load_number`, `load_status`, `order_key`
# - **User & customer**: `user_key`, `customer_key`, `customer_name`
# - **Counts**: `sales_rep_count` (default 1 if null), `customer_count` (default 1 if null)
# - **Sales rep & currency**: `salesrep_id` (lowercased, default `undefined`), `home_currency` (default `USD`)
# - **Costs** *(prefer `ltc`, fallback to `l1`, else 0)*:
#   - `cost` (operating currency)
#   - `cost_usd` (operating → USD)
#   - `cost_cad` (base → CAD)
#   - `cost_currency`
# - **Revenue** *(prefer primary amount; if 0 then use alt)*:
#   - `revenue`
#   - `revenue_usd` (primary vs alt)
#   - `revenue_cad` (primary vs alt)
#   - `revenue_currency`
# - **Exchange rates**:
#   - `exchange_rate_cad_to_usd = l1.exchange_rate_base_to_operating`
#   - `exchange_rate_cad_to_mxn = coalesce(mxn.exchange_rate_cad_to_mxn, 14)` *(MXN fallback = 14)*
# - **AR allocation**: `ar_allocation_status`
# - **Carrier**: `carrier_number`, `carrier_name` (prefer `ltc`)
# - **Branch names**:
#   - `primary_branch_name` — default “Digital LTL” if missing and `organization_id=7`; else `b1.branch_name` or `l1.primary_division_name`
#   - `alternative_branch_name` — “Digital LTL” if `intracompany_load=1`; else `b2.branch_name` or `l1.alternative_division_name`
#   - Corresponding division codes
# - **Delivery & timing**:
#   - Load/order delivery dates & types (from `load_order`)
#   - `created_date_time`, `tender_date_time` (prefer `ltc`), `pickup_date_time`, `delivery_date_time`
#   - `days_diff = datediff(delivery_date, pickup_date)`
# - **Invoice header (from `load_order`)**: `invoice_id`, `invoice_number`, `invoice_status`, `invoice_type`, `invoice_last_modified_date`, `invoice_created_date`
# - **OD addresses**, **distance** *(prefer `ltc.distance` when `l1.distance=0`)*, **equipment**, **service_option**, **mode**, **total_length**
# - **Creator & org info**: `created_by_username` (lower), `organization_id`, `organization_name`, route/pro/bol
# - **Shipping tool**: if `created_by_username = 'shippingappapi'` → “Shipping App”; else “hub.traffix.com”
# - Deduplicates with `.distinct()`.
# 
# ### Currency normalization (home currency)
# Creates **home-currency** versions of revenue and cost:
# - `revenue_home`:
#   - If `home_currency = 'USD'` → `revenue_usd`
#   - If `home_currency = 'CAD'` → `revenue_cad`
#   - If `home_currency = 'MXN'` → `revenue_cad * exchange_rate_cad_to_mxn` *(CAD → MXN)*
#   - Else → `revenue_usd`
# - `cost_home` mirrors the same rules using `cost_usd` / `cost_cad`.
# 
# ### Per‑rep split rules (applied to all measures)
# For each measure \(M ∈ {revenue, revenue_usd, revenue_cad, revenue_home, cost, cost_usd, cost_cad, cost_home}\):
# - If `coalesce(M, 0) = 0` → `M_per_rep = 0`
# - Else if `sales_rep_count > 0` → `M_per_rep = M / sales_rep_count`
# - Else → `M_per_rep = M`
# 
# Creates:
# - `revenue_per_rep`, `revenue_usd_per_rep`, `revenue_cad_per_rep`, `revenue_home_per_rep`
# - `cost_per_rep`, `cost_usd_per_rep`, `cost_cad_per_rep`, `cost_home_per_rep`
# 
# ### Test view
# Filters to a specific load (`load_id = 1879594`) and displays `filtered_df` for validation.


# CELL ********************


base_load_df = (
    l1
    # Organization join (filter deleted)
    .join(organization_df, (F.col("o3.organization_id") == F.col("l1.organization_id")) &
                          (F.col("o3.sys_deleted") == F.lit(0)), "inner")
    # Load allocation (AR/revenue) join
    .join(l2, (F.col("l2.load_id") == F.col("l1.load_id")) &
              (F.col("l2.__sys_deleted") == F.lit(0)), "left")
    # Branch lookups
    .join(b1, (F.col("b1.branch_code") == F.col("l1.primary_division_code")) &
              (F.col("b1.__sys_deleted") == F.lit(0)), "left")
    .join(b2, (F.col("b2.branch_code") == F.col("l1.alternative_division_code")) &
              (F.col("b2.__sys_deleted") == F.lit(0)), "left")
    # LTL/logistics enrichment
    .join(ltc, F.col("ltc.logistics_load_number") == F.col("l1.load_number"), "left")
    # Salesrep assignments
    .join(jf, F.col("jf.load_id") == F.col("l1.load_id"), "left")
    .join(jfc, F.col("jfc.load_id") == F.col("l1.load_id"), "left")
    # Customer identity + counts
    .join(ci, F.col("ci.load_id") == F.col("l1.load_id"), "left")
    .join(cc, F.col("cc.load_id") == F.col("l1.load_id"), "left")
    # User (home_currency, user_key) linked via sales rep
    .join(us, F.col("us.email") == F.col("jf.salesrep_id"), "left")
    # Exchange rates (CAD→MXN via base_to_operating id)
    .join(mxn, F.col("mxn.exchange_rate_id_base_to_operating") == F.col("l1.exchange_rate_id_base_to_operating"), "left")
     # Total_length per load
    .join(d1, F.col("d1.load_id") == F.col("l1.load_id"), "left")
    # Order Delivery Date
    .join(load_order, F.col("ordl.load_id") == F.col("l1.load_id"), "left")
    
    
    
    # Filters
    .where(
        (F.col("l1.created_date") >= F.to_date(F.lit("2023-01-01"))) &
        (F.col("o3.organization_parent_id") == F.lit(4)) &
        (F.col("l1.__sys_deleted") == F.lit(0))
    )

    # Final projection
    .select(
        # Composite primary key: load_id-user_key-customer_key
        F.concat_ws(
            "-",
            F.col("l1.load_id").cast("string"),
            F.coalesce(F.col("us.user_key"), F.lit("undefined")),
            F.coalesce(F.upper(F.col("ci.customer_number")), F.lit("undefined"))
        ).alias("primary_key"),
    
        # Keys and status
        F.col("l1.load_id"),
        F.col("l1.load_number"),
        F.col("l1.load_status"),
        F.col("ordl.order_header_id").alias("order_key"),

        # User & customer keys
        F.coalesce(F.col("us.user_key"), F.lit("undefined")).alias("user_key"),
        F.coalesce(F.upper(F.col("ci.customer_number")), F.lit("undefined")).alias("customer_key"),
        F.coalesce(F.upper(F.col("ci.customer_name")), F.lit("undefined")).alias("customer_name"),
        

        # Counts
        F.coalesce(F.col("jfc.salesrep_count"), F.lit(1)).alias("sales_rep_count"),
        F.coalesce(F.col("cc.customer_count"), F.lit(1)).alias("customer_count"),

        # Sales rep id & currency
        F.coalesce(F.lower(F.col("jf.salesrep_id")), F.lit("undefined")).alias("salesrep_id"),
        F.coalesce(F.col("us.home_currency"), F.lit("USD")).alias("home_currency"),
        


        # Costs (prefer ltc values, fallback to l1, else 0)
        F.coalesce(F.col("ltc.currency_net_cost"),           F.col("l1.currency_net_cost"),           F.lit(0)).alias("cost"),
        F.coalesce(F.col("ltc.currency_operating_net_cost"), F.col("l1.currency_operating_net_cost"), F.lit(0)).alias("cost_usd"),
        F.coalesce(F.col("ltc.currency_base_net_cost"),      F.col("l1.currency_base_net_cost"),      F.lit(0)).alias("cost_cad"),
        F.coalesce(F.col("ltc.currency_code"),               F.col("l1.currency_code"),               F.lit("undefined")).alias("cost_currency"),

        # Revenue (prefer primary amount; if 0 then use alt)
        F.coalesce(F.col("l2.currency_net_charge"), F.col("l2.currency_net_charge_alt"), F.lit(0)).alias("revenue"),
        
        F.when(F.coalesce(F.col("l2.currency_operating_net_charge"), F.lit(0)) == F.lit(0),
               F.coalesce(F.col("l2.currency_operating_net_charge_alt"), F.lit(0))
        ).otherwise(F.coalesce(F.col("l2.currency_operating_net_charge"), F.lit(0))).alias("revenue_usd"),

        F.when(F.coalesce(F.col("l2.currency_base_net_charge"), F.lit(0)) == F.lit(0),
               F.coalesce(F.col("l2.currency_base_net_charge_alt"), F.lit(0))
        ).otherwise(F.coalesce(F.col("l2.currency_base_net_charge"), F.lit(0))).alias("revenue_cad"),

        F.coalesce(F.col("l2.currency_code"), F.lit("undefined")).alias("revenue_currency"),

        # Exchange rates
        F.col("l1.exchange_rate_base_to_operating").alias("exchange_rate_cad_to_usd"),
        F.coalesce(F.col("mxn.exchange_rate_cad_to_mxn"), F.lit(14)).alias("exchange_rate_cad_to_mxn"),

        

        # AR allocation status from load_allocation
        F.col("l2.ar_allocation_status"),

        # Carrier info
        F.coalesce(F.col("ltc.trading_partner_carrier_number"), F.col("l1.trading_partner_carrier_number"), F.lit("undefined")).alias("carrier_number"),
        F.coalesce(F.col("ltc.trading_partner_carrier_name"),   F.col("l1.trading_partner_carrier_name"),   F.lit("undefined")).alias("carrier_name"),

        # Branch names
        F.when(F.col("l1.primary_division_name").isNull() & (F.col("l1.organization_id") == F.lit(7)),
               F.lit("Digital LTL")
        ).otherwise(F.coalesce(F.col("b1.branch_name"), F.col("l1.primary_division_name"))).alias("primary_branch_name"),
        F.col("l1.primary_division_code"),
        

        F.when(F.col("ltc.intracompany_load") == F.lit(1),
               F.lit("Digital LTL")
        ).otherwise(F.coalesce(F.col("b2.branch_name"), F.col("l1.alternative_division_name"))).alias("alternative_branch_name"),
        F.col("l1.alternative_division_code"),


        F.col("ltc.ltl_load_number").alias("intracompany_load_number"),

        # Timestamps (keep as-is; convert to_date(...) if you want date-only)
        F.col("l1.created_date").alias("created_date_time"),
        F.coalesce(F.col("ltc.tender_date"), F.col("l1.tender_date")).alias("tender_date_time"),
        F.col("l1.pickup_date").alias("pickup_date_time"),
        F.col("l1.delivery_date").alias("delivery_date_time"),
        F.datediff(F.col("l1.delivery_date"), F.col("l1.pickup_date")).alias("days_diff"),
        F.col("l1.delivery_date_type").alias("delivery_date_type"),
        F.col("ordl.order_delivery_date").alias("order_delivery_date"),

        #Invoice
        # ---- Invoice header fields ----
        F.col("ordl.invoice_id").alias("invoice_id"),
        F.col("ordl.invoice_number").alias("invoice_number"),
        F.col("ordl.invoice_status").alias("invoice_status"),
        F.col("ordl.invoice_type").alias("invoice_type"),
        F.col("ordl.invoice_last_modified_date").alias("invoice_last_modified_date"),
        F.col("ordl.invoice_created_date").alias("invoice_created_date"),


        # Origins & destinations
        F.col("l1.origin_name"), F.col("l1.origin_address"), F.col("l1.origin_city"),
        F.col("l1.origin_postal_code"), F.col("l1.origin_state"), F.col("l1.origin_country"),
        F.col("l1.destination_name"), F.col("l1.destination_address"), F.col("l1.destination_city"),
        F.col("l1.destination_postal_code"), F.col("l1.destination_state"), F.col("l1.destination_country"),

        # Distance (prefer ltc.distance if l1.distance == 0)
        F.coalesce(
            F.when(F.col("l1.distance") == F.lit(0), F.col("ltc.distance"))
             .otherwise(F.col("l1.distance")),
            F.lit(0)
        ).alias("distance"),

        # Equipment/service/mode
        F.col("l1.equipment_name").alias("equipment"),
        F.col("l1.service_option_name").alias("service_option"),
        F.col("l1.mode_name").alias("mode"),

         # Total length per load
        F.col("d1.total_length"),

        # Creator & org info
        F.lower(F.col("l1.created_by_username")).alias("created_by_username"),
        F.col("l1.organization_id"),
        F.col("l1.organization_name"),
        F.col("l1.route_number"),
        F.col("l1.pro_number"),
        F.col("l1.bol_number"),

        # Shipping tool
        F.when(F.lower(F.col("l1.created_by_username")) == F.lit("shippingappapi"),
               F.lit("Shipping App")
        ).otherwise(F.lit("hub.traffix.com")).alias("shipping_tool")



            )
    .distinct()
)


#SPLIT Revenue per Sales_Prep

base_load_df = (
    base_load_df

    # 1) Revenue normalized to the sales rep's home currency (as in your code)
    .withColumn(
        "revenue_home",
        F.when(F.col("home_currency") == F.lit("USD"), F.col("revenue_usd"))
         .when(F.col("home_currency") == F.lit("CAD"), F.col("revenue_cad"))
         .when(F.col("home_currency") == F.lit("MXN"),
               F.col("revenue_cad") * F.col("exchange_rate_cad_to_mxn"))  # CAD → MXN
         .otherwise(F.col("revenue_usd"))
    )

    # 2) Cost normalized to the sales rep's home currency (mirrors revenue_home)
    .withColumn(
        "cost_home",
        F.when(F.col("home_currency") == F.lit("USD"), F.col("cost_usd"))
         .when(F.col("home_currency") == F.lit("CAD"), F.col("cost_cad"))
         .when(F.col("home_currency") == F.lit("MXN"),
               F.col("cost_cad") * F.col("exchange_rate_cad_to_mxn"))     # CAD → MXN
         .otherwise(F.col("cost_usd"))
    )

    # ---- Per‑rep split rules (same pattern across all measures) ----

    # Revenue family
    .withColumn(
        "revenue_per_rep",
        F.when(F.coalesce(F.col("revenue"), F.lit(0.0)) == F.lit(0.0), F.lit(0.0))
         .when((F.col("sales_rep_count").isNotNull()) & (F.col("sales_rep_count") > F.lit(0)),
               F.col("revenue") / F.col("sales_rep_count"))
         .otherwise(F.col("revenue"))
    )
    .withColumn(
        "revenue_usd_per_rep",
        F.when(F.coalesce(F.col("revenue_usd"), F.lit(0.0)) == F.lit(0.0), F.lit(0.0))
         .when((F.col("sales_rep_count").isNotNull()) & (F.col("sales_rep_count") > F.lit(0)),
               F.col("revenue_usd") / F.col("sales_rep_count"))
         .otherwise(F.col("revenue_usd"))
    )
    .withColumn(
        "revenue_cad_per_rep",
        F.when(F.coalesce(F.col("revenue_cad"), F.lit(0.0)) == F.lit(0.0), F.lit(0.0))
         .when((F.col("sales_rep_count").isNotNull()) & (F.col("sales_rep_count") > F.lit(0)),
               F.col("revenue_cad") / F.col("sales_rep_count"))
         .otherwise(F.col("revenue_cad"))
    )
    .withColumn(  # you already had this; kept for completeness
        "revenue_home_per_rep",
        F.when(F.coalesce(F.col("revenue_home"), F.lit(0.0)) == F.lit(0.0), F.lit(0.0))
         .when((F.col("sales_rep_count").isNotNull()) & (F.col("sales_rep_count") > F.lit(0)),
               F.col("revenue_home") / F.col("sales_rep_count"))
         .otherwise(F.col("revenue_home"))
    )

    # Cost family
    .withColumn(
        "cost_per_rep",
        F.when(F.coalesce(F.col("cost"), F.lit(0.0)) == F.lit(0.0), F.lit(0.0))
         .when((F.col("sales_rep_count").isNotNull()) & (F.col("sales_rep_count") > F.lit(0)),
               F.col("cost") / F.col("sales_rep_count"))
         .otherwise(F.col("cost"))
    )
    .withColumn(
        "cost_usd_per_rep",
        F.when(F.coalesce(F.col("cost_usd"), F.lit(0.0)) == F.lit(0.0), F.lit(0.0))
         .when((F.col("sales_rep_count").isNotNull()) & (F.col("sales_rep_count") > F.lit(0)),
               F.col("cost_usd") / F.col("sales_rep_count"))
         .otherwise(F.col("cost_usd"))
    )
    .withColumn(
        "cost_cad_per_rep",
        F.when(F.coalesce(F.col("cost_cad"), F.lit(0.0)) == F.lit(0.0), F.lit(0.0))
         .when((F.col("sales_rep_count").isNotNull()) & (F.col("sales_rep_count") > F.lit(0)),
               F.col("cost_cad") / F.col("sales_rep_count"))
         .otherwise(F.col("cost_cad"))
    )
    .withColumn(
        "cost_home_per_rep",
        F.when(F.coalesce(F.col("cost_home"), F.lit(0.0)) == F.lit(0.0), F.lit(0.0))
         .when((F.col("sales_rep_count").isNotNull()) & (F.col("sales_rep_count") > F.lit(0)),
               F.col("cost_home") / F.col("sales_rep_count"))
         .otherwise(F.col("cost_home"))
    )
)



# T00128676|302117 -- Test Load
# display(base_load_df)



filtered_df = base_load_df.where(F.col("load_id") == 1837299)

display(filtered_df)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# 
# ## Derive `equipment_group` and classify `service_line`
# 
# Maps free-text `equipment` into normalized groups, sets convenience predicates, then assigns a `service_line` via an ordered rule chain.
# 
# - **Equipment grouping** (fallback mapping):
#   - `Reefer` if `equipment` starts with “reefer”
#   - `Container` if contains “container” or “chassis”
#   - `Intermodal` if contains “intermodal”
#   - `Specialized` if contains any of: “flatbed”, “stepdeck”, “double drop”, “rgn”, “lowboy”, “conestoga”, “special”
#   - `Van` if contains “van”, “dry van”, or “power only”
#   - Else → `Other`
# 
# - **Convenience predicates**:
#   - Orgs: Brokerage (`organization_id=6`), LTL (`organization_id=7`)
#   - Warehouse service: `service_option = 'Warehouse Freight'`
#   - Length bands: `len_gt_24_or_zero` ( >24 or =0 ), `len_ltl_band` (0–24]
#   - Equipment checks: Van/PO, Reefer, Container, Specialized; text-likes for parts/hot/straight/sprinter/air
#   - User flags: non-Traffix creator vs Traffix/“rating” creator
#   - Division: `alternative_division_code = 'EX'` (Expedited)
#   - Customer: names starting with “idexx” → courier logic
# 
# - **`service_line` rules (in precedence order)**:
#   1. **TL Brokerage**: `len_gt_24_or_zero` & Van/PO & Brokerage & not Warehouse
#   2. **LTL Brokerage**: `len_ltl_band` & (Van/PO or parts) & Brokerage & not Warehouse
#   3. **Managed LTL**: LTL org & creator not Traffix
#   4. **Transactional LTL**: LTL org & creator Traffix or “rating”
#   5. **Refrigerated**: Brokerage & Reefer
#   6. **Expedited**: Brokerage & (EX division or hot/straight/sprinter/air)
#   7. **Port Services**: Brokerage & (Container/Chassis or `service_option='Drayage'`)
#   8. **Intermodal**: Brokerage & (`equipment='intermodal'` or `service_option='Intermodal'`)
#   9. **Open Deck**: Brokerage & Specialized
#   10. **Courier**: Customer name like `idexx%`
#   11. **Warehousing**: `service_option='Warehouse Freight'`
#   12. **Other / Unclassified**: default
# 
# > **Note:** Rule order determines classification. Unknown/zero `total_length` with Van/PO will fall into **TL Brokerage**; confirm this is intended. Also, “Power Only” is treated as **Van** in grouping.


# CELL ********************


from pyspark.sql import functions as F

df = base_load_df

# --- derive equipment_group from equipment text (fallback mapping) ---
df = df.withColumn(
    "equipment_group",
    F.when(F.lower(F.col("equipment")).rlike(r"^reefer"), "Reefer")
     .when(F.lower(F.col("equipment")).rlike(r"container|chassis"), "Container")
     .when(F.lower(F.col("equipment")).rlike(r"intermodal"), "Intermodal")
     .when(F.lower(F.col("equipment")).rlike(r"flatbed|stepdeck|double\s*drop|rgn|lowboy|conestoga|special"), "Specialized")
     .when(F.lower(F.col("equipment")).rlike(r"van|dry\s*van|power\s*only"), "Van")
     .otherwise(F.lit("Other"))
)

# Convenience predicates
is_brokerage_org      = (F.col("organization_id") == F.lit(6))
is_ltl_org            = (F.col("organization_id") == F.lit(7))
is_warehouse          = (F.col("service_option") == F.lit("Warehouse Freight"))

len_gt_24_or_zero     = (F.col("total_length") > F.lit(24)) | (F.col("total_length") == F.lit(0))
len_ltl_band          = (F.col("total_length") > F.lit(0)) & (F.col("total_length") <= F.lit(24))

equip_is_van_or_po    = F.col("equipment_group").isin("Van", "Power Only", "Van")  # treat PO as Van
equip_is_reefer       = (F.col("equipment_group") == F.lit("Reefer"))
equip_is_container    = (F.col("equipment_group") == F.lit("Container"))
equip_is_special      = (F.col("equipment_group") == F.lit("Specialized"))

equip_like_parts      = F.lower(F.col("equipment")).like("part%")
equip_like_hot        = F.lower(F.col("equipment")).like("hot%")
equip_like_straight   = F.lower(F.col("equipment")).like("%straight%")
equip_like_sprinter   = F.lower(F.col("equipment")).like("sprinter%")
equip_like_air        = F.lower(F.col("equipment")).like("air%")

user_not_traffix      = ~F.lower(F.col("created_by_username")).like("%@traffix.com%")
user_is_tx_or_rating  = (
    F.lower(F.col("created_by_username")).like("%traffix.com%") |
    F.lower(F.col("created_by_username")).like("%rating%")
)

alt_div_expedited     = (F.col("alternative_division_code") == F.lit("EX"))
cust_is_idexx         = F.lower(F.col("customer_name")).like("idexx%")

# --- derive service_line with WHEN/OTHERWISE chain mirroring your CASE ---
df = df.withColumn(
    "service_line",
    F.when(
        len_gt_24_or_zero & equip_is_van_or_po & is_brokerage_org & ~is_warehouse,
        F.lit("TL Brokerage")
    ).when(
        len_ltl_band & (equip_is_van_or_po | equip_like_parts) & is_brokerage_org & ~is_warehouse,
        F.lit("LTL Brokerage")
    ).when(
        is_ltl_org & user_not_traffix,
        F.lit("Managed LTL")
    ).when(
        is_ltl_org & user_is_tx_or_rating,
        F.lit("Transactional LTL")
    ).when(
        is_brokerage_org & equip_is_reefer,
        F.lit("Refrigerated")
    ).when(
        is_brokerage_org & (alt_div_expedited | equip_like_hot | equip_like_straight | equip_like_sprinter | equip_like_air),
        F.lit("Expedited")
    ).when(
        is_brokerage_org & ~is_warehouse & (equip_is_container | (F.lower(F.col("equipment")) == F.lit("chassis")) | (F.col("service_option") == F.lit("Drayage"))),
        F.lit("Port Services")
    ).when(
        is_brokerage_org & ((F.lower(F.col("equipment")) == F.lit("intermodal")) | (F.col("service_option") == F.lit("Intermodal"))),
        F.lit("Intermodal")
    ).when(
        is_brokerage_org & equip_is_special,
        F.lit("Open Deck")
    ).when(
        cust_is_idexx,
        F.lit("Courier")
    ).when(
        is_warehouse,
        F.lit("Warehousing")
    ).otherwise(F.lit("Other / Unclassified"))
)

# Persist back to base_load_df (or use df as the new base for subsequent joins)
base_load_df = df



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# 
# ## Latest AR per load (rank by last/created timestamps; exclude canceled/no-invoice)
# 
# Selects the **most recent AR record** for each `load_id` from `mrt_ar_df`, using a window ranked by `last_modified_date DESC` then `created_date DESC`. Filters out `Canceled` and `NoInvoice`.
# 
# - **Window**: `w_latest_by_load = PARTITION BY load_id ORDER BY last_modified_date DESC NULLS LAST, created_date DESC NULLS LAST`
# - **Filters**:
#   - `sys_deleted = 0`
#   - `invoice_status_original NOT IN ('Canceled', 'NoInvoice')`
# - **Ranking**:
#   - `rn = row_number()` over the window; keep `rn = 1` as the latest AR per load.
# - **Selected fields**:
#   - **IDs & status**: `ar_header_id`, `ar_number`, `organization_id`, `client_id`, `load_id`, `ar_status`, `ord_billing_status`, `invoice_status_original`
#   - **Timestamps**: `created_date`, `last_modified_date`, `currency_basis_date`
#   - **Operational amounts** (aliased): `ar_net_charge_oper`, `ar_freight_oper`, `ar_accessorial_oper`, `ar_cost_oper`, `ar_thirdparty_oper`, `ar_gainshare_oper`, `ar_shared_savings_oper`
#   - **Base amounts**: `ar_net_charge_base`, `ar_freight_base`, `ar_accessorial_base`, `ar_cost_base`, `ar_thirdparty_base`, `ar_gainshare_base`, `ar_shared_savings_base`
#   - **FX & aging**: `ar_fx_id_b2o`, `ar_fx_b2o`, `days_overdue`, `aging_bucket`, `is_open`, `is_overdue`
# - **Output**:
#   - `latest_ar_by_load_df` containing one AR row per `load_id` (aliased `ar_latest`).
# - **Note**:
#   - This cell uses `sys_deleted` (without underscores). Verify column naming consistency with prior sources that use `__sys_deleted`.
# - **Validation**:
#   - `filtered_df` shows the latest AR for `load_id = 1849229`.
# ``


# CELL ********************


# Window: latest AR per load_id by last_modified_date DESC, then created_date DESC
w_latest_by_load = Window.partitionBy("load_id") \
    .orderBy(F.col("last_modified_date").desc_nulls_last(),
             F.col("created_date").desc_nulls_last())

ar_ranked_df = (
    mrt_ar_df
    .where(
        (F.col("sys_deleted") == F.lit(0)) &
        (~F.col("invoice_status_original").isin("Canceled", "NoInvoice"))
    )
    .withColumn("rn", F.row_number().over(w_latest_by_load))
    .select(
        # Base fields
        F.col("sys_synced"),
        F.col("sys_deleted"),
        F.col("ar_header_id"),
        F.col("ar_number"),
        F.col("organization_id"),
        F.col("client_id"),
        F.col("load_id"),
        F.col("ar_status"),
        F.col("ord_billing_status"),
        F.col("invoice_status_original"),
        F.col("created_date"),
        F.col("last_modified_date"),
        F.col("currency_basis_date"),

        # Operational amounts (aliased like SQL)
        F.col("net_charge_total_oper").alias("ar_net_charge_oper"),
        F.col("net_freight_charge_total_oper").alias("ar_freight_oper"),
        F.col("net_accessorial_charge_total_oper").alias("ar_accessorial_oper"),
        F.col("net_cost_total_oper").alias("ar_cost_oper"),
        F.col("third_party_charge_total_oper").alias("ar_thirdparty_oper"),
        F.col("gainshare_total_oper").alias("ar_gainshare_oper"),
        F.col("shared_savings_rebate_oper").alias("ar_shared_savings_oper"),

        # Base amounts
        F.col("net_charge_total_base").alias("ar_net_charge_base"),
        F.col("net_freight_charge_total_base").alias("ar_freight_base"),
        F.col("net_accessorial_charge_total_base").alias("ar_accessorial_base"),
        F.col("net_cost_total_base").alias("ar_cost_base"),
        F.col("third_party_charge_total_base").alias("ar_thirdparty_base"),
        F.col("gainshare_total_base").alias("ar_gainshare_base"),
        F.col("shared_savings_rebate_base").alias("ar_shared_savings_base"),

        # FX & Aging
        F.col("fx_id_base_to_operating").alias("ar_fx_id_b2o"),
        F.col("fx_base_to_operating").alias("ar_fx_b2o"),
        F.col("days_overdue"),
        F.col("aging_bucket"),
        F.col("is_open"),
        F.col("is_overdue"),

        # Rank
        F.col("rn")
    )
)


latest_ar_by_load_df = (
    ar_ranked_df
    .where(F.col("rn") == F.lit(1))
    .drop("rn")
    .alias("ar_latest")
)

filtered_df = latest_ar_by_load_df.where(F.col("load_id") == 1849229)

display(filtered_df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# 
# ## Invoice raw selection (filter active; alias & field normalization)
# 
# Filters out canceled invoices and selects aliased fields from `mrt_invoice_df` (`inv`) to mirror SQL-friendly naming for downstream joins/analytics.
# 
# - **Filters**:
#   - `sys_deleted = 0`
#   - `invoice_status != 'Canceled'`
# 
# - **Core identity & status**:
#   - IDs & keys: `invoice_id`, `ext_invoice_num`, `invoice_num`, `organization_id`, `client_id`, `owner_code_id`
#   - Lifecycle: `edition`, `invoice_status`, `invoice_type`, `integration_status`
#   - Billing context: `billing_source`, `freight_terms`
#   - Timestamps: `created_date`, `last_modified_date`, `date_invoice`, `currency_basis_date`
# 
# - **Flags**:
#   - Document & reference requirements: `was_invoice_doc_sent`, `are_docs_required`, `bypass_docs`, `are_refnums_required`
#   - Payment/tax: `is_prepayment`, `is_shared_savings_rebate_deducted`, `do_not_override_tax`
# 
# - **Operational (operating currency) amounts**:
#   - `inv_freight_oper`, `inv_accessorial_oper`, `inv_thirdparty_oper`,
#     `inv_shared_savings_oper`, `inv_net_charge_oper`,
#     `inv_tax_oper`, `inv_total_incl_tax_oper`, `inv_paid_oper`
# 
# - **Base currency amounts**:
#   - `net_freight_total_base`, `net_accessorial_total_base`, `third_party_total_base`,
#     `shared_savings_rebate_base`, `inv_net_charge_base`,
#     `tax_total_base`, `inv_total_incl_tax_base`, `inv_paid_base`
# 
# - **FX (base → operating)**:
#   - `inv_fx_id_b2o`, `inv_fx_b2o`
# 
# > Output alias: `inv_raw_df` (aliased as `inv_raw`).  
# > Note: This dataset provides invoice-level charges in both **operating** and **base** currencies, with FX context, suitable for revenue alignment and AR reconciliation.


# CELL ********************

from pyspark.sql import Window as W
# Filter + select with aliases to match your SQL
inv_raw_df = (
    inv.alias("i")
    .where(
        (F.col("i.sys_deleted") == F.lit(0)) &
        (F.col("i.invoice_status").isin("Billed", "Paid", "PaidPartial")) &
        (F.col("i.invoice_type").isin("Original", "Supplemental", "Rebill"))

    )
    .select(
        # Core fields
        F.col("i.sys_synced"),
        F.col("i.sys_deleted"),
        F.col("i.invoice_id"),
        F.col("i.ext_invoice_num"),
        F.col("i.invoice_num"),
        F.col("i.organization_id"),
        F.col("i.client_id"),
        F.col("i.owner_code_id"),
        F.col("i.edition"),
        F.col("i.invoice_status"),
        F.col("i.invoice_type"),
        F.col("i.integration_status"),
        F.col("i.billing_source"),
        F.col("i.freight_terms"),
        F.col("i.created_date"),
        F.col("i.last_modified_date"),
        F.col("i.date_invoice"),
        F.col("i.currency_basis_date"),
        F.col("i.was_invoice_doc_sent"),
        F.col("i.are_docs_required"),
        F.col("i.bypass_docs"),
        F.col("i.are_refnums_required"),
        F.col("i.is_prepayment"),
        F.col("i.is_shared_savings_rebate_deducted"),
        F.col("i.do_not_override_tax"),

        # Operational amounts (aliased)
        F.col("i.net_freight_total_oper").alias("inv_freight_oper"),
        F.col("i.net_accessorial_total_oper").alias("inv_accessorial_oper"),
        F.col("i.third_party_total_oper").alias("inv_thirdparty_oper"),
        F.col("i.shared_savings_rebate_oper").alias("inv_shared_savings_oper"),
        F.col("i.net_charge_total_oper").alias("inv_net_charge_oper"),
        F.col("i.tax_total_oper").alias("inv_tax_oper"),
        F.col("i.total_including_tax_oper").alias("inv_total_incl_tax_oper"),
        F.col("i.paid_total_oper").alias("inv_paid_oper"),

        # Base amounts (aliased where needed)
        F.col("i.net_freight_total_base").alias("net_freight_total_base"),
        F.col("i.net_accessorial_total_base").alias("net_accessorial_total_base"),
        F.col("i.third_party_total_base").alias("third_party_total_base"),
        F.col("i.shared_savings_rebate_base").alias("shared_savings_rebate_base"),
        F.col("i.net_charge_total_base").alias("inv_net_charge_base"),
        F.col("i.tax_total_base").alias("tax_total_base"),
        F.col("i.total_including_tax_base").alias("inv_total_incl_tax_base"),
        F.col("i.paid_total_base").alias("inv_paid_base"),

        # FX
        F.col("i.fx_id_base_to_oper").alias("inv_fx_id_b2o"),
        F.col("i.fx_base_to_oper").alias("inv_fx_b2o"),

        # Json Multi Value 
        F.col("i.ord_header_ids_json").alias("ord_header_ids"),
        F.col("i.ref_numbers_json").alias("ref_numbers"),
        F.col("i.ar_header_ids_json").alias("ar_header_ids")
      
    )
    .alias("inv_raw")
)

from pyspark.sql import functions as F, types as T

amount_cols = [
    "inv_freight_oper","inv_accessorial_oper","inv_thirdparty_oper","inv_shared_savings_oper",
    "inv_net_charge_oper","inv_tax_oper","inv_total_incl_tax_oper","inv_paid_oper",
    "net_freight_total_base","net_accessorial_total_base","third_party_total_base",
    "shared_savings_rebate_base","inv_net_charge_base","tax_total_base",
    "inv_total_incl_tax_base","inv_paid_base",
]

# If ord_header_ids is a JSON string array; if already array, skip from_json
inv_exploded = (
    inv_raw_df
    .withColumn(
        "ord_header_id",
        F.explode_outer(F.from_json(F.col("ord_header_ids"), T.ArrayType(T.StringType())))
    )
)

# Cast numeric amounts to decimal for accurate sums
for c in amount_cols:
    inv_exploded = inv_exploded.withColumn(c, F.col(c).cast("decimal(38,6)"))

inv_sum_by_ord = (
    inv_exploded
    .groupBy("ord_header_id")
    .agg(*[F.coalesce(F.sum(F.col(c)), F.lit(0)).alias(c) for c in amount_cols])
)


inv_exploded_meta = inv_exploded

meta_cols = [
    "invoice_id", "ext_invoice_num", "invoice_num",
    "invoice_status", "invoice_type", "integration_status",
    "date_invoice", "last_modified_date",
    "inv_fx_b2o", "inv_fx_id_b2o",
]

win = W.partitionBy("ord_header_id").orderBy(
    F.col("date_invoice").desc(), F.col("last_modified_date").desc()
)

inv_meta_by_ord = (
    inv_exploded_meta
    .withColumn("rn", F.row_number().over(win))
    .where(F.col("rn") == 1)
    .select("ord_header_id", *[F.col(c) for c in meta_cols])
)

# ---------------------------------------------
# Compose final summarized invoice view keyed by ord_header_id
# ---------------------------------------------
inv_fin = (
    inv_sum_by_ord.alias("s")
    .join(inv_meta_by_ord.alias("m"), on="ord_header_id", how="left")
    .alias("inv_fin")
)

# If your base_load_df.n.order_key is INT and ord_header_id is STRING,
# create an INT version to simplify the join:
inv_fin = inv_fin.withColumn("ord_header_id_int", F.col("ord_header_id").cast("int"))



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(inv_fin)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Validation Query 
filtered_df = inv_fin.where(F.col("invoice_id").isin([1530863,1530868,1485415]) )
display(filtered_df)
#Validation Query 





# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Freight Bill 

# CELL ********************

from pyspark import StorageLevel

# ---------- A1) FREIGHT BILL rows: normalize + anchor groups to Original's ID ----------
fb_rows = (
    df_freight_bill.alias("fb")
    .where((F.col("fb.__sys_deleted") == F.lit(0)))
    .select(
        F.col("fb.load_id"),
        F.col("fb.load_number"),
        F.col("fb.freight_bill_id"),
        F.col("fb.freight_bill_number"),
        F.col("fb.freight_bill_type"),                           # 'Original' | 'Supplemental'
        F.when(F.col("fb.freight_bill_type") == F.lit("Original"),
               F.col("fb.freight_bill_id")
        ).otherwise(F.col("fb.original_freight_bill_id")).alias("group_id"),
        F.col("fb.freight_bill_status"),
        F.col("fb.is_on_hold"),
        F.coalesce(F.col("fb.match_score"), F.lit(0.0)).alias("match_score"),
        F.col("fb.bol_number"),
        F.col("fb.pro_number"),
        # Pay-to surrogate (freight_bill has no pay-to; use carrier)
        F.col("fb.trading_partner_carrier_number").alias("pay_to_number"),
        F.col("fb.trading_partner_carrier_name").alias("pay_to_name"),
        # Operating currency (USD)
        F.coalesce(F.col("fb.currency_net_cost"),     F.lit(0.0)).alias("oper_freight_usd"),
        F.coalesce(F.col("fb.currency_operating_net_accessorial_cost"), F.lit(0.0)).alias("oper_accessorial_usd"),
        F.coalesce(F.col("fb.currency_operating_net_cost"),             F.lit(0.0)).alias("oper_total_usd"),
        F.coalesce(F.col("fb.currency_net_cost_vouchered"),             F.lit(0.0)).alias("vouchered_usd"),
        # FX
        F.col("fb.exchange_rate_base_to_operating").alias("exch_base_to_oper")
    )
    .where(F.col("group_id").isNotNull())
)

# ---------- A2) FREIGHT BILL group per load + pay-to ----------
fb_group = (
    fb_rows
    .groupBy("load_id", "group_id", "pay_to_number", "pay_to_name")
    .agg(
        F.sum(F.when(F.col("freight_bill_type") == "Original", F.lit(1)).otherwise(F.lit(0))).alias("originals_count"),
        F.count(F.lit(1)).alias("rows_in_group"),
        F.sum("oper_freight_usd").alias("fb_freight_usd"),
        F.sum("oper_accessorial_usd").alias("fb_accessorial_usd"),
        F.sum("oper_total_usd").alias("fb_total_usd"),
        F.sum("vouchered_usd").alias("fb_vouchered_usd"),
        F.min("exch_base_to_oper").alias("exch_base_to_oper")
    )
)

# ---------- A3) Enforce your rule: has Original AND fully vouchered ----------
fb_group_valid = (
    fb_group
    .withColumn("has_original", F.when(F.col("originals_count") == F.lit(1), F.lit(1)).otherwise(F.lit(0)))
    .withColumn(
        "fully_vouchered",
        F.when((F.col("fb_total_usd") > 0) & (F.col("fb_vouchered_usd") >= F.col("fb_total_usd")), F.lit(1)).otherwise(F.lit(0))
        # Strict equality alternative:
        # F.when(F.col("fb_total_usd") > 0, F.when(F.col("fb_vouchered_usd") == F.col("fb_total_usd"), 1).otherwise(0)).otherwise(0)
    )
)

# ---------- A4) LOAD COST aggregated per pay-to (fallback source) ----------
lc_payto = (
    df_load_cost.alias("lc")
    .where(
        (F.col("__sys_deleted") == F.lit(0)) &
        (F.col("load_id").isNotNull())  # ignore orphan rows
    )
    .select(
        F.col("load_id"),
        # Prefer pay_to_* if present, else carrier
        F.coalesce(F.col("trading_partner_pay_to_number"), F.col("trading_partner_carrier_number")).alias("pay_to_number"),
        F.coalesce(F.col("trading_partner_pay_to_name"),   F.col("trading_partner_carrier_name")).alias("pay_to_name"),
        F.col("cost_type"),
        F.col("currency_operating_net_cost").alias("lc_oper_usd"),
        F.col("exchange_rate_base_to_operating").alias("lc_exch_base_to_oper")
    )
    .groupBy("load_id", "pay_to_number", "pay_to_name")
    .agg(
        F.sum(F.when(F.col("cost_type") == "Freight",     F.col("lc_oper_usd")).otherwise(F.lit(0.0))).alias("lc_freight_usd"),
        F.sum(F.when(F.col("cost_type") == "Accessorial", F.col("lc_oper_usd")).otherwise(F.lit(0.0))).alias("lc_accessorial_usd"),
        F.sum("lc_oper_usd").alias("lc_total_usd"),
        F.min("lc_exch_base_to_oper").alias("lc_exch_base_to_oper")
    )
)


# ---------- A5 (optimized): Aggregate FB to pay-to, build keys, LEFT join sources, choose ----------
# 5.1 Aggregate Freight Bill groups to PAY-TO level (avoid duplicates later)
fb_payto = (
    fb_group_valid
    .groupBy("load_id", "pay_to_number", "pay_to_name")
    .agg(
        # If ANY group under a pay-to is invalid, we mark the pay-to invalid (AND semantics)
        F.min("has_original").alias("has_original_all_groups"),
        F.min("fully_vouchered").alias("fully_vouchered_all_groups"),

        # Sum components across all FB groups under the same pay-to
        F.sum("fb_freight_usd").alias("fb_freight_usd"),
        F.sum("fb_accessorial_usd").alias("fb_accessorial_usd"),
        F.sum("fb_total_usd").alias("fb_total_usd"),

        # Select a deterministic exchange rate per pay-to (assumes constant per load; if not,
        # using MIN keeps determinism—CAD is only for reporting, not gating)
        F.min("exch_base_to_oper").alias("fb_exch_base_to_oper")
    )
    .persist(StorageLevel.MEMORY_AND_DISK)
)

# 5.2 Build canonical keys across FB and LC (so we can LEFT join both sides efficiently)
keys_df = (
    fb_payto.select("load_id", "pay_to_number", "pay_to_name").distinct()
    .unionByName(lc_payto.select("load_id", "pay_to_number", "pay_to_name").distinct())
    .dropDuplicates(["load_id", "pay_to_number"])          # normalize by number; keep first name
    .persist(StorageLevel.MEMORY_AND_DISK)
)

# 5.3 LEFT join FB and LC to the canonical keys using EQUI-JOINS (fast path)
keys_fb = (
    keys_df.alias("k")
    .join(
        fb_payto.alias("f"),
        on=[
            F.col("k.load_id") == F.col("f.load_id"),
            F.col("k.pay_to_number") == F.col("f.pay_to_number")
        ],
        how="left"
    )
    .select(
        F.col("k.load_id"), F.col("k.pay_to_number"), F.col("k.pay_to_name"),
        F.col("f.has_original_all_groups"),
        F.col("f.fully_vouchered_all_groups"),
        F.col("f.fb_freight_usd"),
        F.col("f.fb_accessorial_usd"),
        F.col("f.fb_total_usd"),
        F.col("f.fb_exch_base_to_oper")

 
    )
    .persist(StorageLevel.MEMORY_AND_DISK)
)

keys_lc = (
    keys_df.alias("k")
    .join(
        lc_payto.alias("l"),
        on=[
            F.col("k.load_id") == F.col("l.load_id"),
            F.col("k.pay_to_number") == F.col("l.pay_to_number")
        ],
        how="left"
    )
    .select(
        F.col("k.load_id"), F.col("k.pay_to_number"), F.col("k.pay_to_name"),
        F.col("l.lc_freight_usd"),
        F.col("l.lc_accessorial_usd"),
        F.col("l.lc_total_usd"),
        F.col("l.lc_exch_base_to_oper")
    )
    .persist(StorageLevel.MEMORY_AND_DISK)
)

# 5.4 Choose per pay-to: FreightBill when (has_original=1 AND fully_vouchered=1), else LoadCost
chosen_payto = (
    keys_fb.alias("a")
    .join(
        keys_lc.alias("b"),
        on=["load_id", "pay_to_number", "pay_to_name"],
        how="left"
    )
    .withColumn(
        "chosen_source",
        F.when(
            (F.col("a.has_original_all_groups") == 1) & (F.col("a.fully_vouchered_all_groups") == 1),
            F.lit("FreightBill")
        ).otherwise(F.lit("LoadCost"))
    )
    .withColumn(
        "usd_to_cad_rate",
        F.when(F.col("a.fb_exch_base_to_oper").isNotNull() & (F.col("a.fb_exch_base_to_oper") != 0),
               1.0 / F.col("a.fb_exch_base_to_oper")
        ).when(F.col("b.lc_exch_base_to_oper").isNotNull() & (F.col("b.lc_exch_base_to_oper") != 0),
               1.0 / F.col("b.lc_exch_base_to_oper")
        )
    )
    .withColumn(
        "chosen_freight_usd",
        F.when(F.col("chosen_source") == "FreightBill", F.col("b.lc_freight_usd"))
         .otherwise(F.col("a.fb_freight_usd"))
    )
    
    .withColumn(
        "chosen_accessorial_usd",
        F.when(F.col("chosen_source") == "FreightBill", F.col("b.lc_accessorial_usd"))
         .otherwise(F.col("a.fb_accessorial_usd"))
        
    )
    .withColumn(
        "chosen_total_usd",
        F.when(F.col("chosen_source") == "FreightBill", F.col("b.lc_total_usd"))
         .otherwise(F.col("a.fb_total_usd"))
        
    )
    .persist(StorageLevel.MEMORY_AND_DISK)
        
)

# ---------- A6 (optimized): Aggregate to load-level once (USD + CAD) ----------
chosen_load = (
    chosen_payto
    .groupBy("load_id")
    .agg(
        F.sum("chosen_freight_usd").alias("fb_layer_freight_usd"),
        F.sum("chosen_accessorial_usd").alias("fb_layer_accessorial_usd"),
        F.sum("chosen_total_usd").alias("fb_layer_total_usd"),
        F.sum(F.col("chosen_freight_usd")     * F.coalesce(F.col("usd_to_cad_rate"), F.lit(0.0))).alias("fb_layer_freight_cad"),
        F.sum(F.col("chosen_accessorial_usd") * F.coalesce(F.col("usd_to_cad_rate"), F.lit(0.0))).alias("fb_layer_accessorial_cad"),
        F.sum(F.col("chosen_total_usd")       * F.coalesce(F.col("usd_to_cad_rate"), F.lit(0.0))).alias("fb_layer_total_cad")
    )
    .alias("fb_cost")
    .persist(StorageLevel.MEMORY_AND_DISK)
)

# (Optional) trigger caches once to avoid recomputation downstream
_ = chosen_load.count()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

filtered_df = chosen_load.where(F.col("load_id") == 2387893)

display(filtered_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

filtered_df = chosen_load.where(F.col("load_id") == 2387893)

display(filtered_df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# 
# ## Layered revenue/cost (Invoice → AR → Load), home-currency normalization, and final load fact
# 
# Joins **latest AR** and **invoice** to the base load, computes **layered revenue/cost** with explicit source priority, derives **home-currency** values (USD/CAD/MXN), and outputs diagnostics, FX, and margins.
# 
# ### 1) Layered expressions (source priority)
# - **Revenue (USD)**: `Invoice.oper → AR.oper → Load.revenue_usd`
#   ```python
#   revenue_usd_expr = coalesce(inv_fin.inv_net_charge_oper, ar_fin.ar_net_charge_oper, n.revenue_usd)
#   revenue_cad_expr = coalesce(inv_fin.inv_net_charge_base, ar_fin.ar_net_charge_base, n.revenue_cad)
#   revenue_mxn_expr = revenue_cad_expr * coalesce(n.exchange_rate_cad_to_mxn, lit(14))
# 
#   


# CELL ********************


from pyspark.sql import functions as F
from pyspark.sql import types as T


def positive_or_null(col):
    # Returns the column only if > 0; otherwise NULL (so COALESCE skips it)
    return F.when(col > 0, col)

# USD: Invoice (oper) → AR (oper) → Load (n.revenue_usd)
revenue_usd_expr = F.coalesce(
    positive_or_null(F.col("inv_fin.inv_net_charge_oper")),
    positive_or_null(F.col("ar_fin.ar_net_charge_oper")),
    F.col("n.revenue_usd")
)

# CAD: Invoice (base) → AR (base) → Load (n.revenue_cad)
revenue_cad_expr = F.coalesce(
    positive_or_null(F.col("inv_fin.inv_net_charge_base")),
    positive_or_null(F.col("ar_fin.ar_net_charge_base")),
    F.col("n.revenue_cad")
)


# MXN path derived from layered CAD
revenue_mxn_expr = revenue_cad_expr * F.coalesce(F.col("n.exchange_rate_cad_to_mxn"), F.lit(14))

# Cost expressions (Invoice has no cost → AR → Load)
cost_usd_expr = F.coalesce(
    F.col("fb_cost.fb_layer_total_usd"),  # FreightBill chosen total (USD) if valid
    F.col("n.cost_usd"),           # AR operating (USD)
    F.col("ar_fin.ar_cost_oper")                     # Load operating (USD)
)

cost_cad_expr = F.coalesce(
    F.col("fb_cost.fb_layer_total_cad"),  # FreightBill chosen total (CAD) if valid
    F.col("n.cost_cad"),           # AR base (CAD)
    F.col("ar_fin.ar_cost_base")                     # Load base (CAD)
)



# Home-currency aware revenue & cost (USD/CAD/MXN)
revenue_home_expr = (
    F.when(F.col("home_currency") == F.lit("USD"), revenue_usd_expr)
     .when(F.col("home_currency") == F.lit("CAD"), revenue_cad_expr)
     .when(F.col("home_currency") == F.lit("MXN"), revenue_usd_expr) #default to USD for now
     .otherwise(revenue_usd_expr)
)

#.when(F.col("home_currency") == F.lit("MXN"),
     #      cost_cad_expr * F.coalesce(F.col("n.exchange_rate_cad_to_mxn"), F.lit(14)))

cost_home_expr = (
    F.when(F.col("home_currency") == F.lit("USD"), cost_usd_expr)
     .when(F.col("home_currency") == F.lit("CAD"), cost_cad_expr)
     .when(F.col("home_currency") == F.lit("MXN"), cost_usd_expr) #default to USD for now
     
     .otherwise(cost_usd_expr)
)


# Multiplier (equal split across sales_rep_count, defensive fallback)
per_rep_multiplier_expr = (
    F.when(F.coalesce(revenue_home_expr, F.lit(0)) == F.lit(0), F.lit(0.0))  # zero revenue → 0 per rep
     .when(F.col("sales_rep_count").isNotNull() & (F.col("sales_rep_count") > F.lit(0)),
           F.lit(1.0) / F.col("sales_rep_count"))                             # equal split
     .otherwise(F.lit(1.0))                                                  # fallback: full amount
)

# Final per‑rep home‑currency revenue
# Split (home-currency) revenue & cost
# revenue_home_per_rep_expr = F.round(revenue_home_expr * per_rep_multiplier_expr, 2)
# cost_home_per_rep_expr    = F.round(cost_home_expr    * per_rep_multiplier_expr, 2)


inv_fin_parsed = inv_raw_df.alias("inv_fin").withColumn(
    "order_header_ids_arr",
    F.from_json(F.col("inv_fin.ord_header_ids"), T.ArrayType(T.IntegerType()))
)


final_load_df = (
    base_load_df.alias("n")
    # Latest AR per load_id
    .join(
        latest_ar_by_load_df.alias("ar_fin"),
        F.col("ar_fin.load_id") == F.col("n.load_id"),
        "left"
    )
    # Invoice via invoice_id already in base load
    .join(
        inv_fin.alias("inv_fin"),
        F.col("inv_fin.ord_header_id_int") == F.col("n.order_key").cast("int"),
        "left"
    )


    .join(chosen_load.alias("fb_cost"),
          F.col("fb_cost.load_id") == F.col("n.load_id"), "left")
    .drop(F.col("fb_cost.load_id"))
    .select(
        # ---- Core keys/metadata from base load ----
        F.col("n.primary_key"),
        F.col("n.load_id"),
        F.col("n.load_number"),
        F.col("n.load_status"),
        F.col("n.user_key"),
        F.col("n.customer_key"),
        F.col("n.order_key"),
        F.col("n.salesrep_id").alias("sales_rep_id"),
        F.col("n.sales_rep_count"),
        F.col("n.customer_count"),
        F.col("n.home_currency"),
        F.col("n.revenue_currency"),
        F.col("n.exchange_rate_cad_to_usd"),
        F.col("n.exchange_rate_cad_to_mxn"),
        F.col("n.ar_allocation_status"),
        F.col("n.primary_division_code"),
        F.col("n.primary_branch_name"),
        F.col("n.alternative_branch_name"),
        F.col("n.alternative_division_code"),
        F.col("n.created_date_time"),
        F.col("n.tender_date_time"),
        F.col("n.delivery_date_time"),
        F.col("n.delivery_date_type"),
        F.col("n.pickup_date_time"),
        F.col("n.order_delivery_date"),
        F.col("n.organization_id"),
        

        # Carrier / routing context
        F.col("n.carrier_number"),
        F.col("n.carrier_name"),
        F.col("n.origin_name"),
        F.col("n.origin_address"),
        F.col("n.origin_city"),
        F.col("n.origin_postal_code"),
        F.col("n.origin_state"),
        F.col("n.origin_country"),
        F.col("n.destination_name"),
        F.col("n.destination_address"),
        F.col("n.destination_city"),
        F.col("n.destination_postal_code"),
        F.col("n.destination_state"),
        F.col("n.destination_country"),
        F.col("n.distance"),
        F.col("n.equipment"),
        F.col("n.equipment_group"),
        F.col("n.service_option"),
        F.col("n.service_line"),
        F.col("n.mode"),
        F.col("n.total_length"),
        F.col("n.route_number"),
        F.col("n.pro_number"),
        F.col("n.bol_number"),
        F.col("n.shipping_tool"),

        # ---- Source level + diagnostics ----
        F.when(F.col("inv_fin.invoice_id").isNotNull(), F.lit("Invoice"))
         .when(F.col("ar_fin.ar_header_id").isNotNull(), F.lit("AR"))
         .otherwise(F.lit("Load")).alias("source_level"),

        F.when(F.col("inv_fin.invoice_id").isNotNull(), F.lit(1))
         .when(F.col("ar_fin.ar_header_id").isNotNull(), F.lit(2))
         .otherwise(F.lit(3)).alias("source_priority_rank"),

        F.col("ar_fin.ar_header_id"),
        F.col("ar_fin.ar_number"),
        F.col("inv_fin.invoice_id"),
        F.col("inv_fin.ext_invoice_num"),
        F.col("inv_fin.invoice_num"),
        F.col("inv_fin.invoice_status"),
        F.col("inv_fin.invoice_type"),
        F.col("inv_fin.integration_status"),

        # Layered source status
        F.coalesce(
            F.col("inv_fin.invoice_status"),
            F.col("ar_fin.ar_status"),
            F.col("n.load_status")
        ).alias("source_status"),

        # Layered document identifiers
        F.coalesce(
            F.col("inv_fin.invoice_id").cast("string"),
            F.col("ar_fin.ar_header_id").cast("string"),
            F.col("n.load_id").cast("string")
        ).alias("source_document_id"),

        F.coalesce(
            F.col("inv_fin.ext_invoice_num"),
            F.col("inv_fin.invoice_num"),
            F.col("ar_fin.ar_number"),
            F.col("n.pro_number"),
            F.col("n.bol_number"),
            F.col("n.load_number")
        ).alias("source_document_number"),

        # Booleans
        F.col("inv_fin.invoice_id").isNotNull().alias("is_invoiced"),
        F.col("ar_fin.ar_header_id").isNotNull().alias("has_ar"),

        # ---- Layered dates ----
        F.coalesce(
            F.col("inv_fin.date_invoice"),
            F.col("ar_fin.currency_basis_date"),
            F.col("n.delivery_date_time"),
            F.col("n.tender_date_time"),
            F.col("n.created_date_time")
        ).alias("revenue_date"),

        F.coalesce(
            F.col("inv_fin.last_modified_date"),
            F.col("ar_fin.last_modified_date"),
            F.col("n.tender_date_time"),
            F.col("n.created_date_time")
        ).alias("activity_date"),

        # ---- Layered revenue (USD/CAD outputs + home-currency 'revenue') ----
        revenue_usd_expr.alias("revenue_usd"),
        revenue_cad_expr.alias("revenue_cad"),
        #---  revenue_home_expr.alias("revenue"),  # home-currency aware (USD/CAD/MXN)
        revenue_home_expr.alias("revenue"),

        # ---- Layered revenue components (oper/base) ----
        F.coalesce(F.col("inv_fin.inv_freight_oper"),     F.col("ar_fin.ar_freight_oper")).alias("freight_oper"),
        F.coalesce(F.col("inv_fin.inv_accessorial_oper"), F.col("ar_fin.ar_accessorial_oper")).alias("accessorial_oper"),
        F.coalesce(F.col("inv_fin.inv_thirdparty_oper"),  F.col("ar_fin.ar_thirdparty_oper"), F.lit(0)).alias("thirdparty_oper"),
        F.coalesce(F.col("inv_fin.inv_shared_savings_oper"), F.col("ar_fin.ar_shared_savings_oper"), F.lit(0)).alias("shared_savings_oper"),

        F.coalesce(F.col("inv_fin.net_freight_total_base"),     F.col("ar_fin.ar_freight_base")).alias("freight_base"),
        F.coalesce(F.col("inv_fin.net_accessorial_total_base"), F.col("ar_fin.ar_accessorial_base")).alias("accessorial_base"),
        F.coalesce(F.col("inv_fin.third_party_total_base"),     F.col("ar_fin.ar_thirdparty_base"), F.lit(0)).alias("thirdparty_base"),
        F.coalesce(F.col("inv_fin.shared_savings_rebate_base"), F.col("ar_fin.ar_shared_savings_base"), F.lit(0)).alias("shared_savings_base"),

        # ---- Invoice-only amounts (NULL when not invoiced) ----
        F.col("inv_fin.inv_net_charge_oper").alias("invoice_net_charge_oper"),
        F.col("inv_fin.inv_tax_oper").alias("tax_oper"),
        F.col("inv_fin.tax_total_base").alias("tax_base"),
        F.col("inv_fin.inv_total_incl_tax_oper").alias("total_incl_tax_oper"),
        F.col("inv_fin.inv_total_incl_tax_base").alias("total_incl_tax_base"),
        F.col("inv_fin.inv_paid_oper").alias("paid_oper"),
        F.col("inv_fin.inv_paid_base").alias("paid_base"),

        # ---- Layered cost (USD/CAD outputs + home-currency 'cost') ----
        cost_usd_expr.alias("cost_usd"),
        cost_cad_expr.alias("cost_cad"),
        cost_home_expr.alias("cost"),

        # ---- FX layered ----
        F.coalesce(
            F.col("inv_fin.inv_fx_b2o"),
            F.col("ar_fin.ar_fx_b2o"),
            F.when(
                F.col("n.revenue_cad").isNotNull() & (F.col("n.revenue_cad") != F.lit(0)),
                F.col("n.revenue_usd") / F.col("n.revenue_cad")
            )
        ).alias("fx_base_to_oper"),
        F.coalesce(F.col("inv_fin.inv_fx_id_b2o"), F.col("ar_fin.ar_fx_id_b2o")).alias("fx_id_base_to_oper"),

        # ---- AR aging / status passthrough ----
        F.col("ar_fin.ord_billing_status"),
        F.col("ar_fin.invoice_status_original"),
        F.col("ar_fin.currency_basis_date"),
        F.col("ar_fin.days_overdue"),
        F.col("ar_fin.aging_bucket"),
        F.col("ar_fin.is_open"),
        F.col("ar_fin.is_overdue"),

        # ---- Diagnostics & comparisons ----
        
        F.col("ar_fin.ar_net_charge_oper").alias("ar_revenue_compare"),
        F.col("n.revenue_usd").alias("load_revenue_compare"),

    
    )
    # Recompute home-currency revenue and per-rep based on LAYERED values
    # .withColumn("revenue_home_layered", revenue_home_expr)
    #.withColumn(
    #    "revenue_home_per_rep_layered",
    #    F.when(F.coalesce(F.col("revenue_home_layered"), F.lit(0)) == F.lit(0), F.lit(0))
    #     .when(F.col("sales_rep_count").isNotNull() & (F.col("sales_rep_count") > F.lit(0)),
    #           F.col("revenue_home_layered") / F.col("sales_rep_count"))
    #     .otherwise(F.col("revenue_home_layered"))
    #)
    # Margins (USD/CAD) using layered amounts
    .withColumn("margin_oper", F.col("revenue_usd") - F.col("cost_usd"))
    .withColumn("margin_base", F.col("revenue_cad") - F.col("cost_cad"))
)
# Simple dropDuplicates by composite key
# final_load_df_dd = final_load_df.dropDuplicates(["primary_key"])






# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

filtered_df = final_load_df.where(F.col("load_id") == 2520945)

display(filtered_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# 
# ## Build **Customer Sales Fact** (per‑rep splits, keys/dates, lanes) + gross profit
# 
# Creates the final fact table by:
# 1) computing an **equal‑split multiplier** across `sales_rep_count`,
# 2) projecting keys, dates, lane, and attributes,
# 3) assigning **per‑rep** revenue/cost in **USD/CAD/Home**, and
# 4) calculating **gross profit** (home + USD/CAD).
# 
# - **Per‑rep multiplier**:
#   - If `revenue_usd = 0` → multiplier `0.0`
#   - Else if `sales_rep_count > 0` → `1.0 / sales_rep_count`
#   - Else → `1.0` (fallback)
# 
# - **Home currency per‑rep splits**:
#   - `revenue = round(revenue_home * multiplier, 2)`
#   - `cost    = round(cost_home * multiplier, 2)`
#   - Home currency (`USD/CAD/MXN`) already derived upstream from layered amounts (Invoice → AR → Load).
# 
# - **USD/CAD per‑rep splits**:
#   - `revenue_usd = round(revenue_usd * multiplier, 2)`
#   - `revenue_cad = round(revenue_cad * multiplier, 2)`
#   - `cost_usd    = round(cost_usd * multiplier, 2)`
#   - `cost_cad    = round(cost_cad * multiplier, 2)`
# 
# - **Gross profit**:
#   - **Home**: `gross_profit = round(revenue - cost, 2)` (uses per‑rep home amounts)
#   - **USD**:  `gross_profit_usd = round(revenue_usd - cost_usd, 2)` (per‑rep)
#   - **CAD**:  `gross_profit_cad = round(revenue_cad - cost_cad, 2)` (per‑rep)
# 
# - **Keys & attributes**:
#   - Cast/order keys: `order_key (string)`, `load_key (string)`
#   - Branch/vendor/service/equipment/org keys: `branch_primary_key`, `branch_alternative_key`, `vendor_key`, `service_option_key`, `equipment_key`, `organization_key`
#   - Lane string: `lane_city_state = "Origin City, ST → Destination City, ST"`
#   - Mode/route: `mode`, `route_number`
#   - Currency & distance: `home_currency`, `distance`
#   - Delivery date type: `delivery_date_type`
# 
# - **Dates (DATE)**:
#   - `created_date`, `tender_date`, `pickup_date`, `delivery_date`, `order_delivery_date` (all derived via `to_date(...)`)
# 
# - **Diagnostics**:
#   - `per_rep_multiplier` (for transparency)
#   - `split_rule_used`: `zero-revenue` | `equal-split` | `fallback-full`
#   - Date attributes for reporting: `month_key (yyyyMM)`, `year`, `week_of_year`
# 
# - **Validation**:
#   - `filtered_df` displays per‑rep `revenue`, `cost`, and `gross_profit` in Home, USD, and CAD for `load_id = 1879594`.
# ``


# CELL ********************


from pyspark.sql import functions as F

# --- Equal-split multiplier across sales_rep_count ---
per_rep_multiplier_expr = (
    F.when(F.col("sales_rep_count") > 0, F.lit(1.0) / F.col("sales_rep_count"))
     .otherwise(F.lit(1.0))
)

# --- Split (home-currency) revenue & cost ---
revenue_home_per_rep_expr = F.round(F.col("revenue") * per_rep_multiplier_expr, 2) \
    if "revenue" in final_load_df.columns \
    else F.round(revenue_home_expr, 2)

cost_home_per_rep_expr = F.round(F.col("cost") * per_rep_multiplier_expr, 2) \
    if "cost" in final_load_df.columns \
    else F.round(cost_home_expr, 2)

# --- Build lane string, with smart null handling ---
origin_city_state = F.concat_ws(", ", F.col("origin_city"), F.col("origin_state"))
dest_city_state   = F.concat_ws(", ", F.col("destination_city"), F.col("destination_state"))
lane_city_state_expr = F.concat_ws(" → ", origin_city_state, dest_city_state)

# --- Date-only projections ---
created_date_expr       = F.to_date(F.col("created_date_time"))
tender_date_expr        = F.to_date(F.col("tender_date_time"))
pickup_date_expr        = F.to_date(F.col("pickup_date_time"))
delivery_date_expr      = F.to_date(F.col("delivery_date_time"))
order_delivery_date_expr= F.to_date(F.col("order_delivery_date"))


# --- Per-rep USD/CAD splits (for gross profit USD/CAD) ---
revenue_usd_per_rep_expr = F.round(F.col("revenue_usd") * per_rep_multiplier_expr, 2)
revenue_cad_per_rep_expr = F.round(F.col("revenue_cad") * per_rep_multiplier_expr, 2)
cost_usd_per_rep_expr    = F.round(F.col("cost_usd")    * per_rep_multiplier_expr, 2)
cost_cad_per_rep_expr    = F.round(F.col("cost_cad")    * per_rep_multiplier_expr, 2)

# --- Gross profit metrics ---
gp_home_expr = F.round(F.col("revenue") - F.col("cost"), 2)
gp_usd_expr  = F.round(F.col("revenue_usd") - F.col("cost_usd"), 2)
gp_cad_expr  = F.round(F.col("revenue_cad") - F.col("cost_cad"), 2)



# If you didn’t carry division codes through base_load_df, keep branch names;
# otherwise use codes from base (recommended).
branch_primary_key_expr   = F.coalesce(F.col("primary_division_code"), F.col("primary_branch_name"))
branch_alternative_key_expr = F.coalesce(F.col("alternative_division_code"), F.col("alternative_branch_name"))

vendor_key_expr        = F.col("carrier_number").cast("string")
organization_key_expr  = F.col("organization_id").cast("string") if "organization_id" in final_load_df.columns else F.lit(None)

# --- Enrich final_load_df with fact table columns ---
customer_sales_fact_df = (
    final_load_df
    # Keys
    .withColumn("order_key", F.col("order_key").cast("integer"))
    .withColumn("load_key",  F.col("load_id").cast("integer"))
    .withColumn("branch_primary_key",   branch_primary_key_expr)
    .withColumn("branch_alternative_key", branch_alternative_key_expr)
    .withColumn("vendor_key", vendor_key_expr)
    .withColumn("service_option_key", F.col("service_option"))
    .withColumn("equipment_key",      F.col("equipment"))
    .withColumn("organization_key",    F.col("organization_id").cast("string"))

    

    # Dates (DATE)
    .withColumn("created_date",       created_date_expr)
    .withColumn("tender_date",        tender_date_expr)
    .withColumn("pickup_date",        pickup_date_expr)
    .withColumn("load_delivery_date", F.to_date(F.col("n.delivery_date_time")))
    .withColumn("order_delivery_date", order_delivery_date_expr)

    # Lane, mode, route
    .withColumn("lane_city_state", lane_city_state_expr)
    .withColumn("mode", F.col("mode"))
    .withColumn("route_number", F.col("route_number"))

    # Currency and distance
    .withColumn("home_currency", F.col("home_currency"))
    .withColumn("distance", F.col("distance"))

    # Delivery date type
    .withColumn("load_delivery_date_type", F.col("delivery_date_type"))

     # Assign split amounts directly  (home-currency)
    .withColumn("revenue_usd", revenue_usd_per_rep_expr)
    .withColumn("revenue_cad", revenue_cad_per_rep_expr)
    .withColumn("cost_usd",cost_usd_per_rep_expr)
    .withColumn("cost_cad",cost_cad_per_rep_expr)
    
    .withColumn("revenue", revenue_home_per_rep_expr)
    .withColumn("cost",    cost_home_per_rep_expr)

    # Gross profit (home currency split) + USD/CAD split metrics
    .withColumn("gross_profit",      gp_home_expr)
    .withColumn("gross_profit_usd",  gp_usd_expr)
    .withColumn("gross_profit_cad",  gp_cad_expr)

    # Optional diagnostics
    .withColumn("per_rep_multiplier", per_rep_multiplier_expr)
    .withColumn("split_rule_used",
        F.when(F.coalesce(F.col("revenue_usd"), F.lit(0)) == F.lit(0), F.lit("zero-revenue"))
         .when(F.col("sales_rep_count").isNotNull() & (F.col("sales_rep_count") > F.lit(0)), F.lit("equal-split"))
         .otherwise(F.lit("fallback-full"))
    )

    # Recommended date attributes for reporting
    .withColumn("month_key", F.date_format(F.col("load_delivery_date"), "yyyyMM"))
    .withColumn("year",      F.year(F.col("load_delivery_date")))
    .withColumn("week_of_year", F.weekofyear(F.col("load_delivery_date")))
    .withColumn("__sys_synced", F.current_timestamp())
)



filtered_df = (
    customer_sales_fact_df
    .where(F.col("load_id") == F.lit(2520945))
    .select(
        F.col("revenue"),
        F.col("cost"),
        F.col("gross_profit"),
        F.col("revenue_cad"),
        F.col("cost_cad"),
        F.col("gross_profit_cad"),
        F.col("revenue_usd"),
        F.col("cost_usd"),
        F.col("gross_profit_usd")
    )
)



display(filtered_df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# 
# ## De-duplicate by `primary_key` and inspect duplicates (note: detection runs **after** dedupe)
# 
# Drops duplicate rows using the **composite `primary_key`** and then attempts to find duplicates.  
# **Important:** Because duplicates are removed first, the subsequent duplicate check on `primary_key` will **always return zero** (each `primary_key` appears once). Use one of the fixes below.
# 
# - **Current steps**:
#   1. `dropDuplicates(["primary_key"])` → keeps a single row per composite key.
#   2. Groups by `primary_key` and counts occurrences where `row_count > 1` (will be empty due to step 1).
#   3. Joins to show duplicate rows (will be empty).


# CELL ********************

from pyspark.sql import functions as F



# Simple dropDuplicates by composite key
customer_sales_fact_dd_df = customer_sales_fact_df.dropDuplicates(["primary_key"])

# Count rows per load_id
dup_load_counts = (
    customer_sales_fact_dd_df
      .groupBy("primary_key")
      .agg(F.count(F.lit(1)).alias("row_count"))
      .where(F.col("row_count") > 1)
)

# Preview duplicate load_ids and how many times they appear
dup_load_counts.orderBy(F.col("row_count").desc()).show(50, truncate=False)

# Display all rows that are duplicates by load_id
df_dup_load_rows = (
    customer_sales_fact_dd_df.join(dup_load_counts.select("primary_key"), on="primary_key", how="inner")
)
display(df_dup_load_rows)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# 
# ## Set Gold output path (customer_sales_fact)
# 
# Defines the target **Delta** table path in the Gold lakehouse for the `customer_sales_fact` output.
# 
# - **Variable**: `gold_table_path_customer_sales_fact = current_lakehouse_gold_abfss + path_target + table_target`
# - **Purpose**: Centralizes the write destination for the final fact dataset.


# CELL ********************

# Parameters
gold_table_path_customer_sales_fact = f"{current_lakehouse_gold_abfss+path_target+table_target}"



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

customer_sales_fact_dd_df.write.format("delta").mode("overwrite").option("overwriteSchema", "True").save(gold_table_path_customer_sales_fact)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Fact Customer Sales V2 - Star Schema

# MARKDOWN ********************

# ### _Target Table Properties_

# CELL ********************

# declare target table properties

table_target_exists=False
table_target_abfss=None
table_target_full_load=True

try:
    # notebookutils.fs.ls(current_lakehouse_silver_abfss+path_target+table_target)
    table_target = "fact_customer_sales_v2"
    table_source =f"{table_target}"+"_source"
    table_target_abfss=spark.read.load(current_lakehouse_gold_abfss+path_target+table_target)
    primary_key="primary_key"
    date_modified="__sys_synced"
    table_target_exists=True
    print(f"{table_target} incremental load will start")
except Exception as e: # can return Path does not exist error if it is first full load
    print(f"{table_target} full load will start since: ",e)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### _Full Query_

# MARKDOWN ********************

# ### _Source Table Properties_

# CELL ********************

mrt_load=spark.read.load(production_lakehouse_silver_abfss+path_source_tgtms_hub+'mrt_load')

dim_customer=spark.read.load(production_lakehouse_gold_abfss+path_source_dimension+'dim_customer')
dim_load=spark.read.load(production_lakehouse_gold_abfss+path_source_dimension+'dim_load')
dim_equipment=spark.read.load(production_lakehouse_gold_abfss+path_source_dimension+'dim_equipment')
dim_division=spark.read.load(production_lakehouse_gold_abfss+path_source_dimension+'dim_division')
dim_user=spark.read.load(production_lakehouse_gold_abfss+path_source_dimension+'dim_user')
dim_primary_sales_rep=spark.read.load(production_lakehouse_gold_abfss+path_source_dimension+'dim_primary_sales_rep')
dim_functional_group=spark.read.load(production_lakehouse_gold_abfss+path_source_dimension+'dim_functional_group')
house_accounts_map = spark.read.load(production_lakehouse_silver_abfss + path_source_lookup +"house_accounts_email_dos_mapping")




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### _Full Write_

# CELL ********************

print(f"{table_target} table full load started...")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F, Window

fg_window = Window.partitionBy("functional_group").orderBy(
    F.when(F.col("subgroup") == "General", 0).otherwise(1),
    F.col("functional_group_key").asc()
)

dim_functional_group_deduped = (
    dim_functional_group
    .withColumn("rn", F.row_number().over(fg_window))
    .filter(F.col("rn") == 1)
    .drop("rn")
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col

df_multi = mrt_load.filter(
    col("sales_rep_count") > 1
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import split, explode, trim, col

df_exploded = df_multi.withColumn(
    "sales_rep_array",
    split(col("sales_rep_id"), ",")
).withColumn(
    "sales_rep_id_exploded",
    explode(col("sales_rep_array"))   # ✅ explode first
).withColumn(
    "sales_rep_id_exploded",
    trim(col("sales_rep_id_exploded"))  # ✅ then trim
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col

dim_customer_lookup = dim_customer.select(
    col("customer_number"),
    col("primary_sales_rep_email").alias("dim_primary_sales_rep_email")
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_joined = df_exploded.join(
    dim_customer_lookup,
    on="customer_number",
    how="left"
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import when
from pyspark.sql.functions import max as spark_max
from pyspark.sql.functions import split, element_at, coalesce

df_valid_primary = df_joined.withColumn(
    "is_valid_primary",
    col("sales_rep_id_exploded") == col("dim_primary_sales_rep_email")
)

df_primary = df_valid_primary.groupBy("load_id").agg(
    spark_max(
        when(col("is_valid_primary"), col("dim_primary_sales_rep_email"))
    ).alias("primary_sales_rep_id")
)

df_fallback = mrt_load.select(
    "load_id",
    split(col("sales_rep_id"), ",").alias("sales_rep_array")
).withColumn(
    "fallback_rep",
    element_at(col("sales_rep_array"), 1)
).select("load_id", "fallback_rep")

df_final_primary = df_primary.join(
    df_fallback,
    on="load_id",
    how="left"
).withColumn(
    "final_sales_rep_id",
    coalesce(col("primary_sales_rep_id"), col("fallback_rep"))
).select("load_id", "final_sales_rep_id")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col
from pyspark.sql.functions import coalesce

house_lookup = house_accounts_map.select(
    col("house_account_email"),
    col("dos_email").alias("mapped_sales_rep_email")
).alias("hl")

df_final_primary = df_final_primary.join(
    house_lookup,
    df_final_primary["final_sales_rep_id"] == house_lookup["house_account_email"],
    how="left"
)

df_final_primary_house = df_final_primary.withColumn(
    "final_sales_rep_id",
    coalesce(col("hl.mapped_sales_rep_email"), col("final_sales_rep_id"))
).drop("house_account_email", "mapped_sales_rep_email")




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mrt_load = mrt_load.join(
    df_final_primary_house.select("load_id", "final_sales_rep_id"),
    on="load_id",
    how="left"
).withColumn(
    "sales_rep_id",
    coalesce(col("final_sales_rep_id"), col("sales_rep_id"))
).drop("final_sales_rep_id")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

# TIX-30119: reassign non-CS org loads (rules from TIX-30118 analysis)
CS_ORG_EXCLUDED_FGS = [
    "ALTERNATE SALES CANADA",
    "ALTERNATE SALES USA",
    "Sales Support 1",
]
KIRAN_USER_KEY = 2096
RYAN_USER_KEY = 3089
FG_ALTERNATE_CANADA = 1
FG_ALTERNATE_USA = 2


def _is_cs_org_member(department_col, functional_group_col):
    return (
        (F.coalesce(department_col, F.lit("")) == F.lit("Customer Sales"))
        & (~F.coalesce(functional_group_col, F.lit("")).isin(CS_ORG_EXCLUDED_FGS))
    )


df_joined = (
    mrt_load.alias("mrt")
    .join(
        dim_customer.alias("c"),
        F.col("mrt.customer_number") == F.col("c.customer_number"),
        "left",
    )
    .join(
        dim_primary_sales_rep.alias("u"),
        F.col("u.primary_key") == F.col("mrt.sales_rep_id"),
        "left",
    )
    .join(
        dim_functional_group_deduped.alias("fg"),
        F.col("u.functional_group") == F.col("fg.functional_group"),
        "left",
    )
    .join(
        dim_user.alias("du"),
        F.lower(F.col("mrt.sales_rep_id")) == F.lower(F.col("du.email")),
        "left",
    )
    .join(
        dim_functional_group_deduped.alias("fg_du"),
        F.col("du.functional_group") == F.col("fg_du.functional_group"),
        "left",
    )
    .join(
        dim_load.alias("l"),
        F.col("mrt.load_number") == F.col("l.load_number"),
        "left",
    )
    .join(
        dim_equipment.alias("e"),
        (F.col("mrt.equipment_id") == F.col("e.equipment_id"))
        & (F.col("e.rated_equipment_flag") == F.lit("Y")),
        "left",
    )
    .join(
        dim_division.alias("dp"),
        F.col("mrt.primary_division_code") == F.col("dp.division_code"),
        "left",
    )
    .join(
        dim_division.alias("da"),
        F.col("mrt.alternative_division_code") == F.col("da.division_code"),
        "left",
    )
    .filter(
        (F.col("mrt.organization_id").isin(6, 7))
        & (F.col("mrt.created_date_time") >= F.lit(history_start_date))
    )
)

is_cs_member = (
    F.when(
        F.col("u.user_key").isNotNull(),
        _is_cs_org_member(F.col("u.department"), F.col("u.functional_group")),
    )
    .when(
        F.col("du.user_key").isNotNull(),
        _is_cs_org_member(F.col("du.department"), F.col("du.functional_group")),
    )
    .otherwise(F.lit(False))
)
resolved_country = F.coalesce(F.col("u.country"), F.col("du.country"))
has_resolved_user = F.col("u.user_key").isNotNull() | F.col("du.user_key").isNotNull()
natural_user_key = F.coalesce(F.col("u.user_key"), F.col("du.user_key"), F.lit(-1))
natural_fg_key = (
    F.when(F.col("u.user_key").isNotNull(), F.coalesce(F.col("fg.functional_group_key"), F.lit(-1)))
    .when(F.col("du.user_key").isNotNull(), F.coalesce(F.col("fg_du.functional_group_key"), F.lit(-1)))
    .otherwise(F.lit(-1))
)
reassign_user_key = (
    F.when(resolved_country == F.lit("Canada"), F.lit(KIRAN_USER_KEY))
    .when(resolved_country.isin("United States", "Mexico"), F.lit(RYAN_USER_KEY))
    .otherwise(F.lit(-1))
)
reassign_fg_key = (
    F.when(resolved_country == F.lit("Canada"), F.lit(FG_ALTERNATE_CANADA))
    .when(resolved_country.isin("United States", "Mexico"), F.lit(FG_ALTERNATE_USA))
    .otherwise(F.lit(-1))
)
reassign_home_currency = (
    F.when(resolved_country == F.lit("Canada"), F.lit("CAD"))
    .when(resolved_country.isin("United States", "Mexico"), F.lit("USD"))
    .otherwise(F.col("u.home_currency"))
)

df_source = df_joined.select(
    F.coalesce(F.col("l.load_key"), F.lit(-1)).cast("int").alias("load_key"),
    F.coalesce(F.col("c.customer_key"), F.lit(-1)).cast("int").alias("customer_key"),
    F.when((~is_cs_member) & has_resolved_user, reassign_user_key)
    .otherwise(natural_user_key)
    .cast("int")
    .alias("primary_sales_rep_key"),
    F.when((~is_cs_member) & has_resolved_user, reassign_fg_key)
    .otherwise(natural_fg_key)
    .cast("int")
    .alias("functional_group_key"),
    F.coalesce(F.col("e.equipment_key"), F.lit(-1)).cast("int").alias("equipment_key"),
    F.coalesce(F.col("dp.division_key"), F.lit(-1)).cast("int").alias("division_key"),
    F.coalesce(F.col("da.division_key"), F.lit(-1)).cast("int").alias("alternate_division_key"),
    F.date_format(F.col("mrt.created_date_time"), "yyyyMMdd").cast("int").alias("create_date_key"),
    F.date_format(F.col("mrt.tender_date_time"), "yyyyMMdd").cast("int").alias("tender_date_key"),
    F.date_format(F.col("mrt.order_delivery_date"), "yyyyMMdd").cast("int").alias("order_delivery_date"),
    F.date_format(F.col("mrt.delivery_date_time"), "yyyyMMdd").cast("int").alias("load_delivery_date"),
    F.col("mrt.revenue_usd"),
    F.col("mrt.revenue_cad"),
    F.col("mrt.revenue_mxn"),
    F.col("mrt.gross_profit_usd"),
    F.col("mrt.gross_profit_cad"),
    F.col("mrt.gross_profit_mxn"),
    F.col("mrt.cost_usd"),
    F.col("mrt.cost_cad"),
    F.col("mrt.cost_mxn"),
    F.when((~is_cs_member) & has_resolved_user, reassign_home_currency)
    .otherwise(F.col("u.home_currency"))
    .alias("home_currency"),
    F.col("mrt.distance"),
    F.current_timestamp().alias("__sys_synced"),
    F.lit(False).cast("boolean").alias("__sys_deleted"),
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_source_clean = (
    df_source
        .withColumn("revenue_usd", F.col("revenue_usd").cast("decimal(18,2)"))
        .withColumn("revenue_cad", F.col("revenue_cad").cast("decimal(18,2)"))
        .withColumn("revenue_mxn", F.col("revenue_mxn").cast("decimal(18,2)"))
        .withColumn("cost_usd", F.col("cost_usd").cast("decimal(18,2)"))
        .withColumn("cost_cad", F.col("cost_cad").cast("decimal(18,2)"))
        .withColumn("cost_mxn", F.col("cost_mxn").cast("decimal(18,2)"))
        .withColumn("gross_profit_usd", F.col("gross_profit_usd").cast("decimal(18,2)"))
        .withColumn("gross_profit_cad", F.col("gross_profit_cad").cast("decimal(18,2)"))
        .withColumn("gross_profit_mxn", F.col("gross_profit_mxn").cast("decimal(18,2)"))
        # TIX-29863: materialize home-currency amounts for dl_customer_sales_v2 Home slicer
        .withColumn(
            "revenue_home",
            F.when(F.col("home_currency") == F.lit("USD"), F.col("revenue_usd"))
             .when(F.col("home_currency") == F.lit("CAD"), F.col("revenue_cad"))
             .when(F.col("home_currency") == F.lit("MXN"), F.col("revenue_mxn"))
        )
        .withColumn(
            "gross_profit_home",
            F.when(F.col("home_currency") == F.lit("USD"), F.col("gross_profit_usd"))
             .when(F.col("home_currency") == F.lit("CAD"), F.col("gross_profit_cad"))
             .when(F.col("home_currency") == F.lit("MXN"), F.col("gross_profit_mxn"))
        )
        .withColumn(
            "cost_home",
            F.when(F.col("home_currency") == F.lit("USD"), F.col("cost_usd"))
             .when(F.col("home_currency") == F.lit("CAD"), F.col("cost_cad"))
             .when(F.col("home_currency") == F.lit("MXN"), F.col("cost_mxn"))
        )
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, substring

df_final = (
    df_source_clean
        .withColumn(
            "create_date_month",
            (col("create_date_key") / 100).cast("int")  # YYYYMM
        )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_final = df_final.dropDuplicates(["load_key"])


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --------------------------------------
# CONFIGURATION
# --------------------------------------
table_name = "fact_customer_sales_v2"

full_path = current_lakehouse_gold_abfss + path_target + table_name
table_target = f"{table_name}"

# --------------------------------------
# FILE SIZE CONTROL (VERY IMPORTANT)
# --------------------------------------
# Target ~150–250MB files
spark.conf.set("spark.sql.files.maxRecordsPerFile", 5_000_000)

# --------------------------------------
# REPARTITION (ENSURES GOOD DISTRIBUTION)
# --------------------------------------
df_final = df_final.repartition(300, "create_date_month")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --------------------------------------
# Repartition (no column)
# --------------------------------------
df_final = df_final.repartition(10)

# --------------------------------------
# Write (NO partitioning ✅)
# --------------------------------------
write_gold_table(
    df=df_final,
    path=current_lakehouse_gold_abfss + path_target + table_target,
    partition_cols=None,   # ✅ REMOVE partitioning
    zorder_cols=["load_key", "customer_key", "primary_sales_rep_key"],
    mode="overwrite",
    run_optimize=True
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.conf.set("spark.sql.files.maxRecordsPerFile", 5_000_000)

df_final = df_final.repartition(300, "create_date_month")

(
    df_final
        .write
        .format("delta")
        .mode("overwrite")
        .partitionBy("create_date_month")
        .option("overwriteSchema", "true")
        .save(current_lakehouse_gold_abfss + path_target + table_target)
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

table_path = current_lakehouse_gold_abfss + path_target + table_target

spark.sql(f"""
OPTIMIZE delta.`{table_path}`
WHERE create_date_month >= '2026-01'
ZORDER BY (load_key, customer_key, primary_sales_rep_key)
""")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

# 2. PATH
table_path = current_lakehouse_gold_abfss + path_target + table_target

# 3. OPTIMIZE
spark.sql(f"""
OPTIMIZE delta.`{table_path}`
""")

# 4. ZORDER
spark.sql(f"""
OPTIMIZE delta.`{table_path}`
ZORDER BY (customer_key, primary_sales_rep_key)
""")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************


spark.sql(f"""
OPTIMIZE delta.`{table_path}`
ZORDER BY (load_key, customer_key, primary_sales_rep_key)
""")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

(
    df_final
        .repartition(200, "create_date_month")   # controls file grouping
        .write
        .format("delta")
        .mode("overwrite")
        .partitionBy("create_date_month")
        .option("overwriteSchema", "true")
        .save(current_lakehouse_gold_abfss + path_target + table_target)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

print(f"{table_target} table full load started...")

df_source = spark.sql(
    query_full,
    mrt_load=mrt_load,
    dim_customer=dim_customer,
    dim_load=dim_load,
    dim_equipment=dim_equipment,
    dim_division=dim_division,
    history_start_date=history_start_date
)

# temp view roundtrip (unchanged)
df_source.createOrReplaceTempView(f"{table_source}")
df_source = spark.sql(f"select * from {table_source}")
spark.catalog.dropTempView(f"{table_source}")

# ✅ SCHEMA NORMALIZATION STEP (NEW)
from pyspark.sql.functions import col

df_source = (
    df_source
        .withColumn("load_key", col("load_key").cast("int"))
        .withColumn("customer_key", col("customer_key").cast("int"))
        .withColumn("equipment_key", col("equipment_key").cast("int"))
        .withColumn("division_key", col("division_key").cast("int"))
        .withColumn("alternate_division_key", col("alternate_division_key").cast("int"))
        .withColumn("create_date_key", col("create_date_key").cast("int"))
        .withColumn("tender_date_key", col("tender_date_key").cast("int"))
        .withColumn("order_delivery_date", col("order_delivery_date").cast("int"))
        .withColumn("load_delivery_date", col("load_delivery_date").cast("int"))
        .withColumn("revenue_usd", col("revenue_usd").cast("decimal(18,2)"))
        .withColumn("revenue_cad", col("revenue_cad").cast("decimal(18,2)"))
        .withColumn("revenue_mxn", col("revenue_mxn").cast("decimal(18,2)"))
        .withColumn("cost_usd", col("cost_usd").cast("decimal(18,2)"))
        .withColumn("cost_cad", col("cost_cad").cast("decimal(18,2)"))
        .withColumn("cost_mxn", col("cost_mxn").cast("decimal(18,2)"))
        .withColumn("gross_profit_usd", col("gross_profit_usd").cast("decimal(18,2)"))
        .withColumn("gross_profit_cad", col("gross_profit_cad").cast("decimal(18,2)"))
        .withColumn("gross_profit_mxn", col("gross_profit_mxn").cast("decimal(18,2)"))
        .withColumn("__sys_synced", col("__sys_synced").cast("timestamp"))
        .withColumn("__sys_deleted", col("__sys_deleted").cast("boolean"))
)

df_source_clean = (
    df_source
    .withColumn("load_key", col("load_key").cast("int"))
    .withColumn("customer_key", col("customer_key").cast("int"))
    .withColumn("equipment_key", col("equipment_key").cast("int"))
    .withColumn("division_key", col("division_key").cast("int"))
    .withColumn("alternate_division_key", col("alternate_division_key").cast("int"))
    .withColumn("create_date_key", col("create_date_key").cast("int"))
    .withColumn("tender_date_key", col("tender_date_key").cast("int"))
    .withColumn("order_delivery_date", col("order_delivery_date").cast("int"))
    .withColumn("load_delivery_date", col("load_delivery_date").cast("int"))
)

# save target dataframe
df_source_clean.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "True") \
    .save(f"{current_lakehouse_gold_abfss+path_target+table_target}")


print(f"{table_target} table full load completed")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# MARKDOWN ********************

# ## Fact Customer Sales Plan

# MARKDOWN ********************

# ### _Target Table Properties_

# CELL ********************

# declare target table properties

table_target_exists=False
table_target_abfss=None
table_target_full_load=True

try:
    # notebookutils.fs.ls(current_lakehouse_silver_abfss+path_target+table_target)
    table_target = "fact_customer_sales_plan"
    table_source =f"{table_target}"+"_source"
    table_target_abfss=spark.read.load(current_lakehouse_gold_abfss+path_target+table_target)
    primary_key="primary_key"
    date_modified="__sys_synced"
    table_target_exists=True
    print(f"{table_target} incremental load will start")
except Exception as e: # can return Path does not exist error if it is first full load
    print(f"{table_target} full load will start since: ",e)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### _Full Query_

# CELL ********************

query_full_ana_plan ="""

SELECT
coalesce(dc.customer_key, -1) as customer_key,
cast(date_format(anaplan.period, 'yyyyMMdd') as int) as sales_plan_date_key,
anaplan.plan_cad,
anaplan.plan_local_currency
FROM {ana_plan} anaplan
inner join {dim_customer} dc
on anaplan.customer_key = dc.customer_number
"""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### _Source Table Properties_

# CELL ********************

dim_customer=spark.read.load(production_lakehouse_gold_abfss+path_source_dimension+'dim_customer')
ana_plan=spark.read.load(production_lakehouse_gold_abfss+path_source_finance+'anaplan_gp_customer')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### _Full Write_

# CELL ********************

print(f"{table_target} table full load started...")

# create target table dataframe. make sure to modify parameters per target table
df_source=spark.sql(
    query_full_ana_plan,
    dim_customer=dim_customer,
    ana_plan=ana_plan
    )
    
# convert dataframe to temporary view first in order to save it later as delta managed table
df_source.createOrReplaceTempView(f"{table_source}")
df_source=spark.sql(f"select * from {table_source}")
spark.catalog.dropTempView(f"{table_source}")

# save target dataframe
df_source.write.format("delta").mode("overwrite").option("overwriteSchema", "True").save(f"{current_lakehouse_gold_abfss+path_target+table_target}")
print(f"{table_target} table full load completed")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
