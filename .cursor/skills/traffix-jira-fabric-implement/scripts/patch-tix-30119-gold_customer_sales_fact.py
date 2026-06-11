from pathlib import Path

SRC = Path(
    r"C:\Users\NelsonFlores\.cursor\projects\C-Users-NELSON-1-AppData-Local-Temp-28ccad13-ecdc-43a3-93ed-22a3e0500162\agent-tools\c77f4abc-43ce-4a5e-aebd-e1b0a744c845.txt"
)
OUT = Path(__file__).with_name("TIX-30119_gold_customer_sales_fact-content.py")

OLD = """from pyspark.sql import functions as F

df_source = (
    mrt_load.alias("mrt")

    # -----------------------------
    # Joins
    # -----------------------------
    .join(
        dim_customer.alias("c"),
        F.col("mrt.customer_number") == F.col("c.customer_number"),
        "left"
    )
    .join(
        dim_primary_sales_rep.alias("u"),
        F.col("u.primary_key") == F.col("mrt.sales_rep_id"),
        "left"
    )
    .join(
        dim_functional_group_deduped.alias("fg"),
        F.col("u.functional_group") == F.col("fg.functional_group"),
        "left"
    )
    .join(
        dim_load.alias("l"),
        F.col("mrt.load_number") == F.col("l.load_number"),
        "left"
    )
    .join(
        dim_equipment.alias("e"),
        (F.col("mrt.equipment_id") == F.col("e.equipment_id")) &
        (F.col("e.rated_equipment_flag") == F.lit("Y")),
        "left"
    )
    .join(
        dim_division.alias("dp"),
        F.col("mrt.primary_division_code") == F.col("dp.division_code"),
        "left"
    )
    .join(
        dim_division.alias("da"),
        F.col("mrt.alternative_division_code") == F.col("da.division_code"),
        "left"
    )

    # -----------------------------
    # Filters
    # -----------------------------
    .filter(
        (F.col("mrt.organization_id").isin(6, 7)) &
        (F.col("mrt.created_date_time") >= F.lit(history_start_date))
    )

    # -----------------------------
    # Select
    # -----------------------------
    .select(
        F.coalesce(F.col("l.load_key"), F.lit(-1)).cast("int").alias("load_key"),
        F.coalesce(F.col("c.customer_key"), F.lit(-1)).cast("int").alias("customer_key"),
        F.coalesce(F.col("u.user_key"), F.lit(-1)).cast("int").alias("primary_sales_rep_key"),
        F.coalesce(F.col("fg.functional_group_key"), F.lit(-1)).cast("int").alias("functional_group_key"),
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
        F.col("u.home_currency"),
        F.col("mrt.distance"),
        F.current_timestamp().alias("__sys_synced"),
        F.lit(False).cast("boolean").alias("__sys_deleted")
    )
)"""

NEW = """from pyspark.sql import functions as F

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
"""


def main():
    text = SRC.read_text(encoding="utf-8")
    if OLD not in text:
        raise SystemExit("Expected df_source block not found — notebook may have changed on main")
    OUT.write_text(text.replace(OLD, NEW, 1), encoding="utf-8")
    print(f"Patched notebook written to {OUT}")


if __name__ == "__main__":
    main()
