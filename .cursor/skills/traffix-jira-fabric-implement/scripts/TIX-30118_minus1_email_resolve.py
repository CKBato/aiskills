import json
import struct
import subprocess

import pyodbc

SERVER = "c37dtetbzmlulmsovead5czqyi-j7bdulap2bhupfy2mnyu3zscna.datawarehouse.fabric.microsoft.com"
DATABASE = "b75886a9-bb96-480c-b029-9452c26eaeec"
SQL_COPT_SS_ACCESS_TOKEN = 1256


def connect():
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
    try:
        cur.execute(sql)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
        print("|".join(cols))
        for row in rows[:50]:
            print("|".join("" if v is None else str(v) for v in row))
    except Exception as exc:
        print(f"ERROR: {exc}")


def main():
    conn = connect()
    cur = conn.cursor()

    run(
        cur,
        "schemas with mrt_load",
        """
        SELECT TABLE_SCHEMA, TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_NAME LIKE '%mrt_load%'
        ORDER BY TABLE_SCHEMA, TABLE_NAME
        """,
    )

    # Common Traffix paths - try silver datamart if exposed
    candidates = [
        ("tgtms_hub.mrt_load", "load_id", "sales_rep_id"),
        ("silver_3gtms_hub.mrt_load", "load_id", "sales_rep_id"),
        ("datamart.mrt_load", "load_id", "sales_rep_id"),
    ]

    for table, load_col, rep_col in candidates:
        run(
            cur,
            f"probe {table}",
            f"SELECT TOP 1 {load_col}, {rep_col} FROM {table}",
        )

    conn.close()


if __name__ == "__main__":
    main()
