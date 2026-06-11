import json
import struct
import subprocess
import sys

import pyodbc

SERVER = "c37dtetbzmlulmsovead5czqyi-j7bdulap2bhupfy2mnyu3zscna.datawarehouse.fabric.microsoft.com"
DATABASE = "b75886a9-bb96-480c-b029-9452c26eaeec"
SQL_COPT_SS_ACCESS_TOKEN = 1256


def get_token() -> str:
    out = subprocess.check_output(
        [
            "az.cmd",
            "account",
            "get-access-token",
            "--resource",
            "https://database.windows.net",
            "-o",
            "json",
        ],
        text=True,
        shell=True,
    )
    return json.loads(out)["accessToken"]


def connect():
    token = get_token()
    token_bytes = token.encode("utf-16-le")
    token_struct = struct.pack(f"<I{len(token_bytes)}s", len(token_bytes), token_bytes)
    conn_str = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={SERVER};DATABASE={DATABASE};"
        "Encrypt=yes;TrustServerCertificate=no;"
    )
    return pyodbc.connect(conn_str, attrs_before={SQL_COPT_SS_ACCESS_TOKEN: token_struct})


def run(cur, title, sql, limit=50):
    print(f"\n=== {title} ===")
    try:
        cur.execute(sql)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
        print("|".join(cols))
        for row in rows[:limit]:
            print("|".join("" if v is None else str(v) for v in row))
        if len(rows) > limit:
            print(f"... {len(rows) - limit} more rows")
    except Exception as exc:
        print(f"ERROR: {exc}")


def main():
    conn = connect()
    cur = conn.cursor()

    run(
        cur,
        "TABLE ROW COUNTS",
        """
        SELECT 'dim_user' AS tbl, COUNT(*) AS cnt FROM dimension.dim_user
        UNION ALL SELECT 'dim_primary_sales_rep', COUNT(*) FROM dimension.dim_primary_sales_rep
        UNION ALL SELECT 'dim_functional_group', COUNT(*) FROM dimension.dim_functional_group
        UNION ALL SELECT 'fact_customer_sales_v2', COUNT(*) FROM customer_sales.fact_customer_sales_v2
        """,
    )

    run(
        cur,
        "dim_primary_sales_rep COLUMNS",
        """
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = 'dimension' AND TABLE_NAME = 'dim_primary_sales_rep'
        ORDER BY ORDINAL_POSITION
        """,
        limit=100,
    )

    run(
        cur,
        "dim_primary_sales_rep country distribution",
        """
        SELECT COALESCE(NULLIF(LTRIM(RTRIM(country)), ''), 'NULL/BLANK') AS country_bucket,
               COUNT(*) AS cnt
        FROM dimension.dim_primary_sales_rep
        GROUP BY COALESCE(NULLIF(LTRIM(RTRIM(country)), ''), 'NULL/BLANK')
        ORDER BY cnt DESC
        """,
    )

    run(
        cur,
        "REASSIGNMENT TARGETS - dim_user",
        """
        SELECT user_key, primary_key, full_name, email, country, functional_group, department, employee_type
        FROM dimension.dim_user
        WHERE LOWER(full_name) LIKE '%kiran%'
           OR LOWER(full_name) LIKE '%ryan%lewin%'
           OR LOWER(full_name) LIKE '%iliyaaz%'
           OR LOWER(full_name) LIKE '%back office%'
           OR LOWER(email) LIKE '%kiran%'
           OR LOWER(email) LIKE '%ryan%'
           OR LOWER(email) LIKE '%iliyaaz%'
        """,
    )

    run(
        cur,
        "REASSIGNMENT TARGETS - dim_functional_group",
        """
        SELECT functional_group_key, functional_group, department, sales_country, operations_country
        FROM dimension.dim_functional_group
        WHERE LOWER(functional_group) LIKE '%alternate canada%'
           OR LOWER(functional_group) LIKE '%alternate us%'
           OR LOWER(functional_group) LIKE '%back office%'
        """,
    )

    run(
        cur,
        "psr.country vs dim_user.country mismatch sample",
        """
        SELECT TOP 20 p.user_key, p.full_name, p.country AS psr_country, u.country AS user_country
        FROM dimension.dim_primary_sales_rep p
        INNER JOIN dimension.dim_user u ON LOWER(p.email) = LOWER(u.email)
        WHERE COALESCE(LTRIM(RTRIM(p.country)), '') <> COALESCE(LTRIM(RTRIM(u.country)), '')
        """,
    )

    run(
        cur,
        "psr.country match summary",
        """
        SELECT
          CASE
            WHEN COALESCE(LTRIM(RTRIM(p.country)), '') = COALESCE(LTRIM(RTRIM(u.country)), '') THEN 'match'
            ELSE 'mismatch'
          END AS country_match,
          COUNT(*) AS cnt
        FROM dimension.dim_primary_sales_rep p
        INNER JOIN dimension.dim_user u ON LOWER(p.email) = LOWER(u.email)
        GROUP BY
          CASE
            WHEN COALESCE(LTRIM(RTRIM(p.country)), '') = COALESCE(LTRIM(RTRIM(u.country)), '') THEN 'match'
            ELSE 'mismatch'
          END
        """,
    )

    run(
        cur,
        "TOP 30 sales reps on fact by load count",
        """
        SELECT TOP 30
          psr.user_key, psr.full_name, psr.email, psr.functional_group, psr.department, u.country,
          COUNT(DISTINCT f.load_key) AS load_count
        FROM customer_sales.fact_customer_sales_v2 f
        LEFT JOIN dimension.dim_primary_sales_rep psr ON f.primary_sales_rep_key = psr.user_key
        LEFT JOIN dimension.dim_user u ON f.primary_sales_rep_key = u.user_key
        GROUP BY psr.user_key, psr.full_name, psr.email, psr.functional_group, psr.department, u.country
        ORDER BY load_count DESC
        """,
    )

    run(
        cur,
        "TOP functional groups on fact",
        """
        SELECT TOP 50
          fg.functional_group, fg.department, fg.sales_country,
          COUNT(DISTINCT f.load_key) AS load_count
        FROM customer_sales.fact_customer_sales_v2 f
        LEFT JOIN dimension.dim_functional_group fg ON f.functional_group_key = fg.functional_group_key
        GROUP BY fg.functional_group, fg.department, fg.sales_country
        ORDER BY load_count DESC
        """,
    )

    run(
        cur,
        "Iliyaaz Ali sample loads",
        """
        SELECT TOP 20 DISTINCT
          f.load_key, psr.full_name, psr.functional_group, psr.department, u.country, fg.functional_group AS fg_name
        FROM customer_sales.fact_customer_sales_v2 f
        LEFT JOIN dimension.dim_primary_sales_rep psr ON f.primary_sales_rep_key = psr.user_key
        LEFT JOIN dimension.dim_user u ON f.primary_sales_rep_key = u.user_key
        LEFT JOIN dimension.dim_functional_group fg ON f.functional_group_key = fg.functional_group_key
        WHERE LOWER(psr.full_name) LIKE '%iliyaaz%'
        """,
    )

    run(
        cur,
        "CS org rule discovery: functional_group x department",
        """
        SELECT TOP 60
          psr.functional_group, psr.department,
          COUNT(DISTINCT f.load_key) AS load_count,
          COUNT(DISTINCT psr.user_key) AS rep_count
        FROM customer_sales.fact_customer_sales_v2 f
        LEFT JOIN dimension.dim_primary_sales_rep psr ON f.primary_sales_rep_key = psr.user_key
        GROUP BY psr.functional_group, psr.department
        ORDER BY load_count DESC
        """,
    )

    run(
        cur,
        "Country distribution for fact-attributed reps",
        """
        SELECT TOP 30
          COALESCE(u.country, 'NULL') AS country,
          COUNT(DISTINCT f.load_key) AS load_count
        FROM customer_sales.fact_customer_sales_v2 f
        LEFT JOIN dimension.dim_user u ON f.primary_sales_rep_key = u.user_key
        GROUP BY COALESCE(u.country, 'NULL')
        ORDER BY load_count DESC
        """,
    )

    conn.close()


if __name__ == "__main__":
    main()
