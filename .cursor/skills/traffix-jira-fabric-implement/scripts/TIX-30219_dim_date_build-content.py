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

# MARKDOWN ********************

# # TIX-30219 — build dim_date (fixed default_* on seed row)

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

path_target = "/Tables/dimension/"
table_target = "dim_date"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pandas as pd
import numpy as np
from pandas.tseries.offsets import DateOffset
from datetime import datetime

start_date = "2020-01-01"
end_date = "2050-12-31"

df = pd.DataFrame({"date": pd.date_range(start_date, end_date)})
df["date_key"] = df["date"].dt.strftime("%Y%m%d").astype(int)

today = pd.Timestamp.now().normalize()
current_year = today.year
current_month = today.month
current_quarter = today.quarter
current_week = today.isocalendar().week

fiscal_shifted = df["date"] + DateOffset(months=6)
current_fiscal = today + DateOffset(months=6)
current_fy = current_fiscal.year
current_fq = current_fiscal.quarter
prev_fq_year = (current_fy - 1) if current_fq == 1 else current_fy
prev_fq = 4 if current_fq == 1 else current_fq - 1

df["date_long_format"] = df["date"].dt.strftime("%B %d, %Y")
df["date_iso_format"] = df["date"].dt.strftime("%Y-%m-%d")
df["day_of_week_name"] = df["date"].dt.day_name()
df["day_of_week_name_short"] = df["day_of_week_name"].str[:3]
df["month_name"] = df["date"].dt.month_name()
df["month_name_short"] = df["month_name"].str[:3]
df["year"] = df["date"].dt.year
df["quarter"] = df["date"].dt.quarter
df["month"] = df["date"].dt.month
df["year_quarter_name"] = df["year"].astype(str) + "Q" + df["quarter"].astype(str)
df["day_of_month"] = df["date"].dt.day
df["day_of_year"] = df["date"].dt.dayofyear
df["day_of_quarter"] = (df["date"].dt.dayofyear - 1) % 91 + 1
df["week_of_year"] = df["date"].dt.isocalendar().week.astype("int64")
df["week_of_quarter"] = (df["day_of_quarter"] - 1) // 7 + 1
df["month_of_quarter"] = (df["month"] - 1) % 3 + 1
df["date_year_month"] = df["date"].dt.strftime("%Y-%m")
df["date_year_month_full"] = df["date"].dt.strftime("%Y-%b")
df["date_year_month_sort"] = df["date"].dt.year * 100 + df["date"].dt.month
df["days_in_month"] = df["date"].dt.days_in_month
df["days_in_year"] = df["date"].dt.is_leap_year.astype(int) + 365
df["days_in_quarter"] = (df["date"] + pd.offsets.QuarterEnd(0)).dt.day
df["month_start_date"] = df["date"].values.astype("datetime64[M]")
df["month_end_date"] = df["month_start_date"] + pd.offsets.MonthEnd(1)
df["week_start_date"] = df["date"] - pd.to_timedelta(df["date"].dt.weekday, unit="D")
df["week_end_date"] = df["week_start_date"] + pd.Timedelta(days=6)
df["first_day_of_month_flag"] = df["date"].dt.is_month_start.astype(int)
df["last_day_of_month_flag"] = df["date"].dt.is_month_end.astype(int)
df["is_today_flag"] = (df["date"] == today).astype(int)
df["is_current_week_flag"] = (
    (df["week_of_year"] == current_week) & (df["year"] == current_year)
).astype(int)
df["is_current_month_flag"] = (
    (df["month"] == current_month) & (df["year"] == current_year)
).astype(int)
df["is_current_quarter_flag"] = (
    (df["quarter"] == current_quarter) & (df["year"] == current_year)
).astype(int)
df["is_currentYear_flag"] = (df["year"] == current_year).astype(int)
df["is_weekend_flag"] = (df["date"].dt.weekday >= 5).astype(int)
df["previous_day"] = df["date"] - DateOffset(days=1)
df["previous_month_day"] = df["date"] - DateOffset(months=1)
df["previous_year_day"] = df["date"] - DateOffset(years=1)
df["next_day"] = df["date"] + DateOffset(days=1)
df["next_month_day"] = df["date"] + DateOffset(months=1)
df["next_year_day"] = df["date"] + DateOffset(years=1)
df["fiscal_year"] = fiscal_shifted.dt.year
df["fiscal_quarter"] = fiscal_shifted.dt.quarter
df["fiscal_month"] = fiscal_shifted.dt.month
df["fiscal_month_of_quarter"] = (df["fiscal_month"] - 1) % 3 + 1
df["fiscal_day_of_month"] = fiscal_shifted.dt.day
df["fiscal_day_of_year"] = fiscal_shifted.dt.dayofyear
df["fiscal_day_of_quarter"] = (fiscal_shifted.dt.dayofyear - 1) % 91 + 1
df["fiscal_week_of_year"] = fiscal_shifted.dt.isocalendar().week.astype("int64")
df["fiscal_week_of_quarter"] = (df["fiscal_day_of_quarter"] - 1) // 7 + 1
df["fiscal_quarter_name"] = (
    df["fiscal_year"].astype(str) + "Q" + df["fiscal_quarter"].astype(str)
)
df["fiscal_year_month"] = fiscal_shifted.dt.strftime("%Y-%m")

df["default_calendar_day"] = np.where(
    df["date"] > today,
    "",
    np.where(df["date"] == today, "Today", df["date"].dt.strftime("%Y-%m-%d")),
)
df["default_calendar_month"] = np.where(
    df["date"] > today.replace(day=1) + pd.offsets.MonthEnd(0),
    "",
    np.where(
        (df["year"] == current_year) & (df["month"] == current_month),
        "Current Month",
        df["date"].dt.strftime("%Y-%m"),
    ),
)
df["default_fiscal_period"] = np.where(
    fiscal_shifted > current_fiscal.replace(day=1) + pd.offsets.MonthEnd(0),
    "",
    np.where(
        (fiscal_shifted.dt.year == current_fy) & (fiscal_shifted.dt.month == current_fiscal.month),
        "Current Month",
        fiscal_shifted.dt.strftime("%Y-%m"),
    ),
)
df["default_fiscal_quarter"] = np.where(
    (fiscal_shifted.dt.year == current_fy) & (fiscal_shifted.dt.quarter == current_fq),
    "Current",
    np.where(
        (fiscal_shifted.dt.year == prev_fq_year) & (fiscal_shifted.dt.quarter == prev_fq),
        "Previous",
        np.where(
            fiscal_shifted > (current_fiscal - DateOffset(months=((current_fq - 1) * 3))),
            "",
            df["fiscal_quarter_name"],
        ),
    ),
)

df["__sys_synced"] = pd.Timestamp.now()
df["__sys_deleted"] = 0

df_source = spark.createDataFrame(df)

# TIX-30219: compute default_* for seed date using same logic (not literal "Undefined")
seed_date = pd.Timestamp("1900-01-01")
seed_fiscal = seed_date + DateOffset(months=6)

if seed_date > today:
    seed_default_calendar_day = ""
elif seed_date == today:
    seed_default_calendar_day = "Today"
else:
    seed_default_calendar_day = seed_date.strftime("%Y-%m-%d")

if seed_date > today.replace(day=1) + pd.offsets.MonthEnd(0):
    seed_default_calendar_month = ""
elif (seed_date.year == current_year) and (seed_date.month == current_month):
    seed_default_calendar_month = "Current Month"
else:
    seed_default_calendar_month = seed_date.strftime("%Y-%m")

if seed_fiscal > current_fiscal.replace(day=1) + pd.offsets.MonthEnd(0):
    seed_default_fiscal_period = ""
elif (seed_fiscal.year == current_fy) and (seed_fiscal.month == current_fiscal.month):
    seed_default_fiscal_period = "Current Month"
else:
    seed_default_fiscal_period = seed_fiscal.strftime("%Y-%m")

seed_fiscal_quarter_name = f"{seed_fiscal.year}Q{seed_fiscal.quarter}"
if (seed_fiscal.year == current_fy) and (seed_fiscal.quarter == current_fq):
    seed_default_fiscal_quarter = "Current"
elif (seed_fiscal.year == prev_fq_year) and (seed_fiscal.quarter == prev_fq):
    seed_default_fiscal_quarter = "Previous"
elif seed_fiscal > (current_fiscal - DateOffset(months=((current_fq - 1) * 3))):
    seed_default_fiscal_quarter = ""
else:
    seed_default_fiscal_quarter = seed_fiscal_quarter_name

undefined_row = {
    "date": datetime(1900, 1, 1),
    "date_key": -1,
    "date_long_format": "Undefined",
    "date_iso_format": "Undefined",
    "day_of_week_name": "Undefined",
    "day_of_week_name_short": "Undefined",
    "month_name": "Undefined",
    "month_name_short": "Undefined",
    "year": -1,
    "quarter": -1,
    "month": -1,
    "year_quarter_name": "Undefined",
    "day_of_month": -1,
    "day_of_year": -1,
    "day_of_quarter": -1,
    "week_of_year": -1,
    "week_of_quarter": -1,
    "month_of_quarter": -1,
    "date_year_month": "Undefined",
    "date_year_month_full": "Undefined",
    "date_year_month_sort": -1,
    "days_in_month": -1,
    "days_in_year": -1,
    "days_in_quarter": -1,
    "month_start_date": datetime(1900, 1, 1),
    "month_end_date": datetime(1900, 1, 1),
    "week_start_date": datetime(1900, 1, 1),
    "week_end_date": datetime(1900, 1, 1),
    "first_day_of_month_flag": -1,
    "last_day_of_month_flag": -1,
    "is_today_flag": -1,
    "is_current_week_flag": -1,
    "is_current_month_flag": -1,
    "is_current_quarter_flag": -1,
    "is_currentYear_flag": -1,
    "is_weekend_flag": -1,
    "previous_day": datetime(1900, 1, 1),
    "previous_month_day": datetime(1900, 1, 1),
    "previous_year_day": datetime(1900, 1, 1),
    "next_day": datetime(1900, 1, 1),
    "next_month_day": datetime(1900, 1, 1),
    "next_year_day": datetime(1900, 1, 1),
    "fiscal_year": -1,
    "fiscal_quarter": -1,
    "fiscal_month": -1,
    "fiscal_month_of_quarter": -1,
    "fiscal_day_of_month": -1,
    "fiscal_day_of_year": -1,
    "fiscal_day_of_quarter": -1,
    "fiscal_week_of_year": -1,
    "fiscal_week_of_quarter": -1,
    "fiscal_quarter_name": "Undefined",
    "fiscal_year_month": "Undefined",
    "default_calendar_day": seed_default_calendar_day,
    "default_calendar_month": seed_default_calendar_month,
    "default_fiscal_period": seed_default_fiscal_period,
    "default_fiscal_quarter": seed_default_fiscal_quarter,
    "__sys_synced": datetime.now(),
    "__sys_deleted": 0,
}

undefined_values = [undefined_row.get(field.name) for field in df_source.schema.fields]
undefined_spark_df = spark.createDataFrame([tuple(undefined_values)], schema=df_source.schema)
df_source = undefined_spark_df.unionByName(df_source)

target_path = f"{current_lakehouse_gold_abfss}{path_target}{table_target}"
df_source.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(target_path)

print("Wrote dim_date to:", target_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

seed = spark.read.load(target_path).filter("date_key = -1")
seed.select(
    "date_key",
    "date",
    "default_calendar_day",
    "default_calendar_month",
    "default_fiscal_period",
    "default_fiscal_quarter",
).show(truncate=False)

print("Row count:", spark.read.load(target_path).count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
