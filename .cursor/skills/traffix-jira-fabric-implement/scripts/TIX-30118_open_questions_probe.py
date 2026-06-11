import json
import struct
import subprocess

import pyodbc

SERVER = "c37dtetbzmlulmsovead5czqyi-j7bdulap2bhupfy2mnyu3zscna.datawarehouse.fabric.microsoft.com"
DATABASE = "b75886a9-bb96-480c-b029-9452c26eaeec"
SQL_COPT_SS_ACCESS_TOKEN = 1256


def connect():
    out = subprocess.check_output(
        ["az.cmd", "account", "get-access-token", "--resource", "https://database.windows.net", "-o", "json"],
        text=True,
        shell=True,
    )
    token = json.loads(out)["accessToken"]
    token_bytes = token.encode("utf-16-le")
    token_struct = struct.pack(f"<I{len(token_bytes)}s", len(token_bytes), token_bytes)
    conn_str = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={SERVER};DATABASE={DATABASE};"
        "Encrypt=yes;TrustServerCertificate=no;"
    )
    return pyodbc.connect(conn_str, attrs_before={SQL_COPT_SS_ACCESS_TOKEN: token_struct})


def run(cur, title, sql):
    print(f"\n=== {title} ===")
    cur.execute(sql)
    cols = [d[0] for d in cur.description]
    print("|".join(cols))
    for row in cur.fetchall()[:30]:
        print("|".join("" if v is None else str(v) for v in row))


def main():
    conn = connect()
    cur = conn.cursor()
    run(
        cur,
        "dim_user fuzzy back/office",
        """
        SELECT user_key, full_name, email, functional_group, department
        FROM dimension.dim_user
        WHERE LOWER(functional_group) LIKE '%office%'
           OR LOWER(department) LIKE '%office%'
           OR LOWER(functional_group) LIKE '%back%'
           OR LOWER(department) LIKE '%back%'
        """,
    )
    run(
        cur,
        "Mexico reps on fact",
        """
        SELECT psr.full_name, psr.functional_group, psr.country, COUNT(DISTINCT f.load_key) load_count
        FROM customer_sales.fact_customer_sales_v2 f
        JOIN dimension.dim_primary_sales_rep psr ON f.primary_sales_rep_key = psr.user_key
        WHERE psr.country = 'Mexico'
        GROUP BY psr.full_name, psr.functional_group, psr.country
        ORDER BY load_count DESC
        """,
    )
    run(
        cur,
        "Sales Support 1 reps",
        """
        SELECT psr.user_key, psr.full_name, psr.country
        FROM dimension.dim_primary_sales_rep psr
        WHERE psr.functional_group = 'Sales Support 1'
        """,
    )
    conn.close()


if __name__ == "__main__":
    main()
