"""Patch gold_dimension_v2_batch_01 dim_date seed row default_* fields for TIX-30219."""
from pathlib import Path
import sys

MARKER = '''df_source = spark.createDataFrame(df)

# ------------------------------------------------------------------
# Undefined Record
# ------------------------------------------------------------------


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

    "default_calendar_day": "Undefined",
    "default_calendar_month": "Undefined",
    "default_fiscal_period": "Undefined",
    "default_fiscal_quarter": "Undefined",

    "__sys_synced": datetime.now(),
    "__sys_deleted": 0
}'''

REPLACEMENT = '''df_source = spark.createDataFrame(df)

# ------------------------------------------------------------------
# Undefined Record (TIX-30219: default_* use computed values for seed date)
# ------------------------------------------------------------------
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
    "__sys_deleted": 0
}'''


def main():
    src = Path(sys.argv[1])
    dst = Path(sys.argv[2]) if len(sys.argv) > 2 else src
    text = src.read_text(encoding="utf-8")
    if MARKER not in text:
        raise SystemExit("Patch marker not found in dim_date section")
    if text.count(MARKER) != 1:
        raise SystemExit(f"Expected exactly 1 marker match, found {text.count(MARKER)}")
    dst.write_text(text.replace(MARKER, REPLACEMENT, 1), encoding="utf-8")
    print(f"Patched {dst}")


if __name__ == "__main__":
    main()
