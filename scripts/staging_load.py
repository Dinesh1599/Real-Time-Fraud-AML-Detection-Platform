import oracledb, pandas as pd, os
from pathlib import Path

# ORACLE CONNECTION CONFIG

USER = os.getenv("ORACLE_APP_USER", "APPUSER")
PWD  = os.getenv("ORACLE_APP_PWD",  "apppwd")
DSN  = os.getenv("ORACLE_DSN",      "localhost/XEPDB1")

# DATA LOCATION
DATA_DIR = Path(os.getenv("DATA_DIR"),"/resources/datasets")
FILES = {
    "customers": str(DATA_DIR / "customers.csv"),
    "branches": str(DATA_DIR / "branches.csv"),
    "accounts": str(DATA_DIR / "accounts.csv"),
    "merchants": str(DATA_DIR / "merchants.csv"),
    "devices": str(DATA_DIR / "devices.csv"),
    "geos": str(DATA_DIR / "geos.csv"),
    "transactions": str(DATA_DIR / "transactions.csv"),
    "logins": str(DATA_DIR / "logins.csv"),
    "sanctions": str(DATA_DIR / "sanctions.csv"),
    "alerts": str(DATA_DIR / "alerts.csv"),
}

def load_csv(conn,name,path):
    print(f'Loading {name} from {path}')
    seperator= "," if str(path).lower().endswith(".csv") else "\t"  # basically uses "," as a seperator if the file is a.csv else uses \t as default
    df  = pd.read_csv(path, sep=seperator, low_memory=False, encoding_errors="ignore")
    cols = list(df.columns)
    table_name = f"stg_{name}"
    columnNames = ", ".join([f'"{c}" VARCHAR2(4000)' for c in cols])  # Extract Table column names with adding variable default to 
    createSQL_query = f'CREATE TABLE {table_name} ({columnNames})'
    cur = conn.cursor()
    try:
        cur.execute(createSQL_query)
        print(f'Created Table {table_name}')
    except oracledb.DatabaseError as e:
        if "name is already used" in str(e).lower() or "ORA-00955" in str(e):
            print(f"Table {table_name} exists; truncating")
            cur.execute(f"TRUNCATE TABLE {table_name}")
        else:
            raise
    
    #Insert records into table
    col_rows = ", " .join([f'"{c}" VARCHAR2(4000)' for c in cols]) #  eg: customer_id VARCHAR2(4000)
    placeholder = ", ".join([f":{i+1}" for i in range(len(cols))])
    insert_sql = f'INSERT INTO {table_name} ("'+'")'


def main():
    with oracledb.connect(user=USER, password=PWD, dsn = DSN) as conn:
       for name, path in FILES.items:
           load_csv(conn,name, path)
           

if __name__ == "__main__":
    main()
