# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse_name": "lh_gold",
# META       "default_lakehouse_workspace_id": "b8a11494-b8e8-407f-af9c-5c360b143bd5"
# META     },
# META     "environment": {}
# META   }
# META }

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

import json
import traceback
from datetime import datetime
from pyspark.sql import functions as F

result = {"workspace": "aiagent-dev-workspace", "pr": 1389, "ticket": "TIX-30119", "validated_at": datetime.utcnow().isoformat() + "Z"}

try:
    path_dim = "/Tables/dimension/"
    path_fact = "/Tables/customer_sales/"

    psr = spark.read.load(production_lakehouse_gold_abfss + path_dim + "dim_primary_sales_rep")
    fg = spark.read.load(production_lakehouse_gold_abfss + path_dim + "dim_functional_group")

    def enrich(fact_df):
        return (
            fact_df.alias("f")
            .join(psr.alias("p"), F.col("f.primary_sales_rep_key") == F.col("p.user_key"), "left")
            .join(fg.alias("g"), F.col("f.functional_group_key") == F.col("g.functional_group_key"), "left")
        )

    prod_fact = spark.read.load(production_lakehouse_gold_abfss + path_fact + "fact_customer_sales_v2")
    dev_fact = spark.read.load(current_lakehouse_gold_abfss + path_fact + "fact_customer_sales_v2")
    prod_enriched = enrich(prod_fact)
    dev_enriched = enrich(dev_fact)

    baseline = {
        "sales_support_1_rows": int(prod_enriched.filter(F.col("g.functional_group") == "Sales Support 1").count()),
        "fg62_rows": int(prod_enriched.filter(F.col("f.functional_group_key") == 62).count()),
        "ryan_us_sales1_rows": int(
            prod_enriched.filter(F.col("f.primary_sales_rep_key") == 3089)
            .filter(F.col("g.functional_group") == "US Sales 1")
            .count()
        ),
        "total_rows": int(prod_fact.count()),
    }

    after = {
        "sales_support_1_rows": int(dev_enriched.filter(F.col("g.functional_group") == "Sales Support 1").count()),
        "fg62_rows": int(dev_enriched.filter(F.col("f.functional_group_key") == 62).count()),
        "ryan_fg2_rows": int(
            dev_enriched.filter(F.col("f.functional_group_key") == 2)
            .filter(F.col("f.primary_sales_rep_key") == 3089)
            .count()
        ),
        "ryan_us_sales1_rows": int(
            dev_enriched.filter(F.col("f.primary_sales_rep_key") == 3089)
            .filter(F.col("g.functional_group") == "US Sales 1")
            .count()
        ),
        "total_rows": int(dev_fact.count()),
    }

    excluded = ["Sales Support 1", "ALTERNATE SALES CANADA", "ALTERNATE SALES USA"]
    excluded_counts = {}
    for name in excluded:
        excluded_counts[name] = int(
            dev_enriched.filter(F.col("g.functional_group") == name)
            .filter(~F.col("f.primary_sales_rep_key").isin(2096, 3089))
            .count()
        )

    checks = {
        "sales_support_1_zero": after["sales_support_1_rows"] == 0,
        "fg62_zero": after["fg62_rows"] == 0,
    }
    for name, cnt in excluded_counts.items():
        checks[f"excluded_{name.replace(' ', '_')}_zero"] = cnt == 0

    result.update(
        {
            "status": "metrics_ok",
            "baseline": baseline,
            "after": after,
            "excluded_counts": excluded_counts,
            "checks": checks,
            "assertions_passed": all(checks.values()),
        }
    )

    try:
        samples = (
            dev_enriched.filter(F.col("f.functional_group_key").isin(1, 2))
            .filter(F.col("f.primary_sales_rep_key").isin(2096, 3089))
            .select(
                F.col("f.load_key").alias("load_key"),
                F.col("f.primary_sales_rep_key").alias("primary_sales_rep_key"),
                F.col("p.full_name").alias("sales_rep_name"),
                F.col("f.functional_group_key").alias("functional_group_key"),
                F.col("g.functional_group").alias("functional_group"),
                F.col("f.customer_key").alias("customer_key"),
            )
            .orderBy(F.desc("load_key"))
            .limit(15)
            .collect()
        )
        top_reps = (
            dev_enriched.groupBy("f.primary_sales_rep_key", "p.full_name", "g.functional_group")
            .count()
            .orderBy(F.desc("count"))
            .limit(10)
            .collect()
        )
        result.update(
            {
                "status": "ok",
                "samples": [r.asDict() for r in samples],
                "top_reps": [r.asDict() for r in top_reps],
            }
        )
    except Exception as sample_err:
        result.update({"sample_error": str(sample_err), "status": "ok_metrics_only"})

    out_dir = current_lakehouse_gold_abfss + "/Files/validation"
    notebookutils.fs.mkdirs(out_dir)
    notebookutils.fs.put(out_dir + "/TIX-30119_PR1389_summary.json", json.dumps(result, indent=2, default=str), True)

    print("BASELINE", baseline)
    print("AFTER", after)
    print("EXCLUDED", excluded_counts)
    print("CHECKS", checks)
    print("ASSERTIONS_PASSED", all(checks.values()))
    if result.get("samples"):
        for row in result["samples"][:5]:
            print("SAMPLE", row)

except Exception as e:
    result.update({"status": "error", "error": str(e), "traceback": traceback.format_exc()})
    out_dir = current_lakehouse_gold_abfss + "/Files/validation"
    notebookutils.fs.mkdirs(out_dir)
    notebookutils.fs.put(out_dir + "/TIX-30119_PR1389_summary.json", json.dumps(result, indent=2, default=str), True)
    print("ERROR", e)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
