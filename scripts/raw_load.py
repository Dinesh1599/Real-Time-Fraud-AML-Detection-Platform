import os, oracledb, pandas as pd
from dotenv import load_dotenv

load_dotenv()

USER = os.getenv("ORACLE_APP_USER", "ADMIN")
PWD  = os.getenv("ORACLE_APP_PWD",  "Dr@gonfire1599")
DSN  = '''(description= (retry_count=20)(retry_delay=3)(address=(protocol=tcps)(port=1521)(host=adb.us-chicago-1.oraclecloud.com))(connect_data=(service_name=g9e10c5aa27d741_oracletxn_high.adb.oraclecloud.com))(security=(ssl_server_dn_match=yes)))'''
DATA_DIR = os.getenv("DATA_DIR")

FILES = {
    "customers": os.path.join(DATA_DIR,"customers_raw.csv"), 
    "branches": os.path.join(DATA_DIR,"branches_raw.csv"), 
    "accounts": os.path.join(DATA_DIR,"accounts_raw.csv"), 
    "merchants": os.path.join(DATA_DIR,"merchants_raw.csv"), 
    "devices": os.path.join(DATA_DIR,"devices_raw.csv"), 
    "geos": os.path.join(DATA_DIR,"geos_raw.csv"), 
    "transactions": os.path.join(DATA_DIR,"transactions_raw.csv"), 
    "logins": os.path.join(DATA_DIR,"logins_raw.csv"), 
    "sanctions": os.path.join(DATA_DIR,"sanctions_raw.csv"),  
    # "alerts": os.path.join(DATA_DIR,"alerts_raw.csv"),
}

print("DATA_DIR:", DATA_DIR)

def create_raw_table(cur, table_name, cols) -> None:
    meta = [
        '"ingest_ts" TIMESTAMP DEFAULT SYSTIMESTAMP',
        '"source_file" VARCHAR2(400)',
        '"rownum_in_file" NUMBER'
    ]
    src_cols = [f'"{c}" VARCHAR2(4000)' for c in cols]
    create_sql = f'CREATE TABLE {table_name} (\n  ' + ",\n  ".join(meta + src_cols) + "\n)"
    print(create_sql)
    
    try:
        cur.execute(create_sql)
        print(f"[RAW] Created {table_name}")
    except oracledb.DatabaseError as e:
        msg = str(e).lower()
        if "ora-00955" in msg or "name is already used" in msg:
            print(f"Table  {table_name} exists;")
            # drop_sql = f'DROP TABLE {table_name}'
            # print(drop_sql)
            # cur.execute(drop_sql)
        else:
            raise

def insert_raw(cur, table_name: str, df: pd.DataFrame, path) -> None:
    cols = list(df.columns)
    extras = ['source_file','rownum_in_file']
    cols = cols + extras
    quoted_cols = '","'.join(cols)
    bind_list   = ", ".join([f":{c}" for c in cols])
    df["source_file"] = path
    df["rownum_in_file"] = range(1, len(df) + 1)
    print(df)
    insert_sql  = f'INSERT INTO {table_name} ("{quoted_cols}") VALUES ({bind_list})'
    print(insert_sql)
    cur.executemany(insert_sql, df)
    print(f"Inserted {len(df)} rows into {table_name}")
    

def load_file(conn, name, path):
    sep = "," if str(path).lower().endswith(".csv") else "\t"
    df = pd.read_csv(path, sep=sep, low_memory=False, encoding_errors="ignore")
    cols = [str(c) for c in df.columns]
    table_name = f'RAW_{name}'

    cur = conn.cursor()
    create_raw_table(cur, table_name, cols)
    insert_raw(cur, table_name, df, path)
    conn.commit()

def main():
    print("DATA_DIR:", DATA_DIR)
    with oracledb.connect(user=USER, password=PWD, dsn=DSN) as conn:
        for name, path in FILES.items():
            print(f"\n=== Loading {name} from {path} ===")
            load_file(conn, name, path)
        conn.commit()
        print("\n[OK] RAW landing complete.")

if __name__ == "__main__":
    main()
