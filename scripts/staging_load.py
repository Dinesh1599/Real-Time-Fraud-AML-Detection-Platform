import os
import oracledb
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv


# Load environment variables
load_dotenv()

USER = os.getenv("ORACLE_APP_USER", "APPUSER")
PWD  = os.getenv("ORACLE_APP_PWD",  "apppwd")
DSN  = os.getenv("ORACLE_DSN",      "localhost/XEPDB1")
DATA_DIR = os.getenv("DATA_DIR")


# File map (Path objects)
FILES = {
    "customers": os.path.join(DATA_DIR,"customers.csv"), 
    "branches": os.path.join(DATA_DIR,"branches.csv"), 
    "accounts": os.path.join(DATA_DIR,"accounts.csv"), 
    "merchants": os.path.join(DATA_DIR,"merchants.csv"), 
    "devices": os.path.join(DATA_DIR,"devices.csv"), 
    "geos": os.path.join(DATA_DIR,"geos.csv"), 
    "transactions": os.path.join(DATA_DIR,"transactions.csv"), 
    "logins": os.path.join(DATA_DIR,"logins.csv"), 
    "sanctions": os.path.join(DATA_DIR,"sanctions.csv"),  
    "alerts": os.path.join(DATA_DIR,"alerts.csv"),
}

print("DATA_DIR:", DATA_DIR)

def load_csv(conn, name, path):
    print(f"Loading {name} from {path}")
    sep = "," if str(path).lower().endswith(".csv") else "\t"
    df = pd.read_csv(path, sep=sep, low_memory=False, encoding_errors="ignore")

    # Normalize column names (strip whitespace, force string)
    df.columns = [str(c).strip() for c in df.columns]
    cols = list(df.columns)

    table_name = f"stg_{name}"

    # Create staging table with VARCHAR2 columns (safe landing)
    col_defs = ", ".join([f'"{c}" VARCHAR2(4000)' for c in cols])
    create_sql = f'CREATE TABLE {table_name} ({col_defs})'

    cur = conn.cursor()
    try:
        cur.execute(create_sql)
        print(f"Created table {table_name}")
    except oracledb.DatabaseError as e:
        msg = str(e).lower()
        if "ora-00955" in msg or "name is already used" in msg:
            print(f"Table {table_name} exists; truncating")
            cur.execute(f"TRUNCATE TABLE {table_name}")
        else:
            raise

    # -------- Named binds --------
    quoted_cols = '","'.join(cols)
    bind_list   = ", ".join([f":{c}" for c in cols])
    insert_sql  = f'INSERT INTO {table_name} ("{quoted_cols}") VALUES ({bind_list})'
    print("INSERT preview:", insert_sql)

    # Convert NaN to None so they go to NULL in Oracle
    rows = df.where(pd.notna(df), None).to_dict("records")

    cur.executemany(insert_sql, rows)
    conn.commit()
    print(f"Inserted {len(df)} rows into {table_name}")

def main():
    with oracledb.connect(user=USER, password=PWD, dsn=DSN) as conn:
        for name, path in FILES.items():
            load_csv(conn, name, path)

if __name__ == "__main__":
    main()
