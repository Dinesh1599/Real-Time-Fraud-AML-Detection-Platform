# BATCH PROCESSING
"""
BATCH PROCESSING INCLUDES FOR THE FOLLOWING FILES THAT HAS ALREADY BEEN LOADED INTO ORACLE DATABASE
customers - done
accounts - done
merchants - done
branches - done
geos - done
"""

import argparse, ipaddress, os, re, oracledb, pandas as pd, sqlalchemy, numpy as np
from dataclasses import dataclass, field
from typing import Dict, List, Optional
from dotenv import load_dotenv

load_dotenv()


USER = os.getenv("ORACLE_APP_USER", "APPUSER")
PWD  = os.getenv("ORACLE_APP_PWD",  "apppwd")
DSN  = os.getenv("ORACLE_DSN",      "localhost/XEPDB1")

def cleanStr(x):
    if pd.isna(x): return None
    return " ".join(str(x).strip().split())

def phoneFix(p):
    if not p or (isinstance(p, float) and np.isnan(p)):return None
    digits = "".join(filter(str.isdigit, str(p)))
    return f"+{digits}" if len(digits) == 11 and digits.startswith("1") else \
        f"+1{digits}" if len(digits) == 10 else None

def parse_date(x: Optional[str]) -> Optional[pd.Timestamp]:
    if pd.isna(x):
        return None
    try:
        d = pd.to_datetime(x, utc=False, errors="coerce").date()
        return pd.Timestamp(d) if pd.notna(d) else None
    except Exception:
        return None

def normalize_rows(rs):
    norm = []
    for r in rs:
        d = {}
        for k, v in r.items():
            d[str(k)] = None if pd.isna(v) else v
        norm.append(d)
    return norm


def clean_time(val):
    try:
        if pd.isna(val) or val ==  "":
            return "1970-01-01T00:00:00" # Default Epoch Date
        else:
            return pd.to_datetime(val, errors="coerce").strftime("%Y-%m-%dT%H:%M:%S%z")
    except Exception:
        return "1970-01-01T00:00:00" # Default Epoch Date

def stg_customer(engine,cur):
    sql_query = "SELECT * FROM RAW_CUSTOMERS"  #change * to the columns you requre - faster
    df = pd.read_sql(sql_query,engine)
    df = df.copy()
    df['customer_id'] = df['customer_id'].str.upper().str.strip()
    df["name"] = df["name"].apply(lambda s: cleanStr(s).title() if s is not None else None)
    df["dob"] = df["dob"].apply(parse_date)
    df["kyc_status"] = df["kyc_status"].astype(str).str.strip().str.upper()
    df["email"] = df["email"].astype(str).str.strip().str.lower()
    df["phone"] = df["phone"].apply(phoneFix)
    df["address"] = df["address"].apply(cleanStr)
    df["zip"] = df["zip"].astype(int)

    df = (
            df.sort_values(["customer_id"])
            .groupby("customer_id", as_index=False)
            .agg({
                "name": "first",
                "dob": "first",
                "kyc_status": "first",
                "email": "first",
                "phone": "first",
                "address": "first",
                "city": "first",
                "state": "first",
                "zip": "first",
                "country": "first"
            })
        )
    
    sql_create_query = """CREATE TABLE STG_CUSTOMER (
        customer_id   VARCHAR2(20) PRIMARY KEY,
        name          VARCHAR2(200),
        dob           DATE,
        kyc_status    VARCHAR2(20),
        email         VARCHAR2(200),
        phone         VARCHAR2(15),
        address       VARCHAR2(400),
        city          VARCHAR2(60),
        state         VARCHAR2(60),
        zip           NUMBER(7),
        country       VARCHAR2(60)
    )"""
    
    try:
        cur.execute(sql_create_query)
        print(f"[STG] Created STG_CUSTOMER")
    except oracledb.DatabaseError as e:
            msg = str(e).lower()
            if "ora-00955" in msg or "name is already used" in msg:
                print(f"Table  STG_CUSTOMER exists;")
                # drop_sql = f'DROP TABLE STG_CUSTOMER'
                # print(drop_sql)
                # cur.execute(drop_sql)
            else:
                raise
    
    rows = df.to_dict(orient="records")
    rows = normalize_rows(rows)
    
    sql_insert_query = """
MERGE INTO STG_CUSTOMER d
USING (
  SELECT
    :customer_id AS customer_id,
    :name        AS name,
    :dob AS dob,
    :kyc_status  AS kyc_status,
    :email       AS email,
    :phone       AS phone,
    :address     AS address,
    :city        AS city,
    :state       AS state,
    :zip         AS zip,
    :country     AS country
  FROM dual
) s
ON (d.customer_id = s.customer_id)
WHEN MATCHED THEN UPDATE SET
    d.name        = s.name,
    d.dob         = s.dob,
    d.kyc_status  = s.kyc_status,
    d.email       = s.email,
    d.phone       = s.phone,
    d.address     = s.address,
    d.city        = s.city,
    d.state       = s.state,
    d.zip         = s.zip,
    d.country     = s.country
WHEN NOT MATCHED THEN INSERT (
  customer_id, name, dob, kyc_status, email, phone, address, city, state, zip, country
) VALUES (
  s.customer_id, s.name, s.dob, s.kyc_status, s.email, s.phone, s.address, s.city, s.state, s.zip, s.country
)
    """
    
    # for i, row in enumerate(rows, start =1):
    #     try:
    #         cur.execute(sql_insert_query,row)
    #     except Exception as e:
    #         print(f"❌ Error on row {i}: {row}")
    #         print(e)
    #         break

    
    cur.executemany(sql_insert_query,rows)
    print("Data loaded successfully into Oracle!")

def stg_account(engine,cur):
    sql_query = "SELECT * FROM RAW_ACCOUNTS"
    df = pd.read_sql_query(sql_query,engine)
    df = df.copy()

    df["account_id"] = df["account_id"].astype(str).str.strip().str.upper()
    df["customer_id"] = df["customer_id"].astype(str).str.strip().str.upper()
    df["type"] = df["type"].astype(str).str.strip().str.title()
    df["opened_at"] = df["opened_at"].apply(clean_time)
    df["branch_id"] = df["branch_id"].astype(str).str.strip().str.upper()
    df["balance"] = df["balance"].astype(float).round(2)
    
    df = df.drop(['ingest_ts','source_file','rownum_in_file'],axis=1)
    
    df.drop_duplicates()

    sql_create_query = """CREATE TABLE STG_ACCOUNTS (
        account_id    VARCHAR2(20) PRIMARY KEY,
        customer_id   VARCHAR2(20) NOT NULL,
        type          VARCHAR2(30),
        balance       NUMBER(10,2),
        currency      VARCHAR2(4),
        status        VARCHAR2(20),
        opened_at     DATE,
        branch_id     VARCHAR2(20),
        CONSTRAINT fk_acc_cust
            FOREIGN KEY (customer_id) REFERENCES STG_CUSTOMER(customer_id)
    )
    """
    try:
        cur.execute(sql_create_query)
        print(f"[STG] Created STG_ACCOUNTS")
    except oracledb.DatabaseError as e:
            msg = str(e).lower()
            if "ora-00955" in msg or "name is already used" in msg:
                print(f"Table  STG_ACCOUNTS exists;")
                # drop_sql = f'DROP TABLE STG_ACCOUNTS'
                # print(drop_sql)
                # cur.execute(drop_sql)
            else:
                raise    

    rows = df.to_dict(orient="records")
    rows = normalize_rows(rows) # to avoid TypeError: Expected str, got quoted_name

    sql_insert_query = """
    MERGE INTO STG_ACCOUNTS d
    USING (
        SELECT
        :account_id  AS account_id,
        :customer_id AS customer_id,
        :type        AS type,
        :currency    AS currency,
        :balance     AS balance,
        :status      AS status,
        TO_TIMESTAMP(:opened_at, 'YYYY-MM-DD"T"HH24:MI:SS') AS opened_at,
        :branch_id   AS branch_id
    FROM dual
    ) s
    ON (d.account_id = s.account_id)
    WHEN MATCHED THEN UPDATE SET
        d.customer_id  = s.customer_id,
        d.type         = s.type,
        d.currency     = s.currency,
        d.balance      = s.balance,
        d.status       = s.status,
        d.opened_at    = s.opened_at,
        d.branch_id    = s.branch_id
    WHEN NOT MATCHED THEN INSERT (
        account_id, customer_id, type, currency, balance, status, opened_at, branch_id
    ) VALUES (
        s.account_id, s.customer_id, s.type, s.currency, s.balance, s.status, s.opened_at, s.branch_id
    )
    """
    cur.executemany(sql_insert_query,rows)
    print("Data loaded successfully into Oracle!")

def stg_merchant(engine,cur):
    sql_query = "SELECT * FROM RAW_MERCHANTS"
    df = pd.read_sql_query(sql_query,engine)
    df = df.copy()

    df = df.drop(['ingest_ts','source_file','rownum_in_file'],axis=1)
    df["merchant_id"] = df["merchant_id"].astype(str).str.strip().str.upper()
    df["name"] = df["name"].apply(lambda s: cleanStr(s).title() if s is not None else None)
    df["category"] = df["category"].apply(lambda s: cleanStr(s).title() if s is not None else None)

    sql_create_query = """
    CREATE TABLE STG_MERCHANTS (
        merchant_id    VARCHAR2(20) PRIMARY KEY,
        name           VARCHAR2(50) NOT NULL,
        mcc            NUMBER(6),
        category       VARCHAR2(50),
        city           VARCHAR2(20),
        state          VARCHAR2(2),
        country_code   VARCHAR2(2)
    )
    """
    try:
        cur.execute(sql_create_query)
        print(f"[STG] Created STG_MERCHANTS")
    except oracledb.DatabaseError as e:
            msg = str(e).lower()
            if "ora-00955" in msg or "name is already used" in msg:
                print(f"Table  STG_MERCHANTS exists;")
                # drop_sql = f'DROP TABLE STG_MERCHANTS'
                # print(drop_sql)
                # cur.execute(drop_sql)
            else:
                raise  
    
    df.drop_duplicates()
    rows = df.to_dict(orient="records")
    rows = normalize_rows(rows) # to avoid TypeError: Expected str, got quoted_name

    

    sql_insert_query = """
    MERGE INTO STG_MERCHANTS d
    USING (
        SELECT
        :merchant_id    AS merchant_id,
        :name           AS name,
        :mcc            AS mcc,
        :category       AS category,
        :city           AS city,
        :state          AS state,
        :country_code   AS country_code
    FROM dual
    ) s
    ON (d.merchant_id = s.merchant_id)
    WHEN MATCHED THEN UPDATE SET
        d.name          = s.name,
        d.mcc           = s.mcc,
        d.category      = s.category,
        d.city          = s.city,
        d.state         = s.state,
        d.country_code  = s.country_code
    WHEN NOT MATCHED THEN INSERT (
        merchant_id, name, mcc, category, city, state, country_code
    ) VALUES (
        s.merchant_id, s.name, s.mcc, s.category, s.city, s.state, s.country_code
    )
    """
    cur.executemany(sql_insert_query,rows)
    print("Data loaded successfully into Oracle!")

def stg_branches(engine,cur):
    sql_query = "SELECT * FROM RAW_BRANCHES"
    df = pd.read_sql_query(sql_query,engine)
    df = df.copy()

    df = df.drop(['ingest_ts','source_file','rownum_in_file'],axis=1)
    df["branch_id"] = df["branch_id"].astype(str).str.strip().str.upper()
    df["name"] = df["name"].apply(lambda s: cleanStr(s).title() if s is not None else None)

    
    df.drop_duplicates()
    rows = df.to_dict(orient="records")
    rows = normalize_rows(rows) # to avoid TypeError: Expected str, got quoted_name

    sql_create_query = """
    CREATE TABLE STG_BRANCHES (
        branch_id       VARCHAR2(20) PRIMARY KEY,
        name            VARCHAR2(50) NOT NULL,
        city            VARCHAR2(20),
        state           VARCHAR2(2),
        country         VARCHAR2(20)
    )
    """
    try:
        cur.execute(sql_create_query)
        print(f"[STG] Created STG_BRANCHES")
    except oracledb.DatabaseError as e:
            msg = str(e).lower()
            if "ora-00955" in msg or "name is already used" in msg:
                print(f"Table  STG_BRANCHES exists;")
                # drop_sql = f'DROP TABLE STG_BRANCHES'
                # print(drop_sql)
                # cur.execute(drop_sql)
            else:
                raise  

    sql_insert_query = """
    MERGE INTO STG_BRANCHES d
    USING (
        SELECT
        :branch_id      AS branch_id,
        :name           AS name,
        :city           AS city,
        :state          AS state,
        :country        AS country
    FROM dual
    ) s
    ON (d.branch_id = s.branch_id)
    WHEN MATCHED THEN UPDATE SET
        d.name          = s.name,
        d.city          = s.city,
        d.state         = s.state,
        d.country       = s.country
    WHEN NOT MATCHED THEN INSERT (
        branch_id, name, city, state, country
    ) VALUES (
        s.branch_id, s.name, s.city, s.state, s.country
    )
    """
    cur.executemany(sql_insert_query,rows)
    print("Data branch loaded successfully into Oracle!")

def stg_geo(engine,cur):
    sql_query = "SELECT * FROM RAW_GEOS"
    df = pd.read_sql_query(sql_query,engine)
    df = df.copy()

    df = df.dropna(subset=["geo_id"])
    df = df.drop(['ingest_ts','source_file','rownum_in_file'],axis=1)
    df["geo_id"] = df["geo_id"].astype(str).str.strip().str.upper()
    df['lat'] = pd.to_numeric(df['lat'], errors='coerce')
    df['lon'] = pd.to_numeric(df['lon'], errors='coerce')
    df = df.drop_duplicates(subset=["geo_id"], keep="first") 

    rows = df.to_dict(orient="records")
    rows = normalize_rows(rows) # to avoid TypeError: Expected str, got quoted_name

    sql_create_query = """
        CREATE TABLE STG_GEOS (
            geo_id         VARCHAR2(20) PRIMARY KEY,
            ip             VARCHAR2(15) NOT NULL,
            city           VARCHAR2(20),
            region         VARCHAR2(2),
            country        VARCHAR2(20),
            lat            NUMBER(9,6),
            lon            NUMBER(9,6)
        )
    """
    try:
        cur.execute(sql_create_query)
        print(f"[STG] Created STG_GEOS")
    except oracledb.DatabaseError as e:
            msg = str(e).lower()
            if "ora-00955" in msg or "name is already used" in msg:
                print(f"Table  STG_GEOS exists;")
                # drop_sql = f'DROP TABLE STG_GEOS'
                # print(drop_sql)
                # cur.execute(drop_sql)
            else:
                raise  

    sql_insert_query = """
    MERGE INTO STG_GEOS d
    USING (
        SELECT
        :geo_id    AS geo_id,
        :ip        AS ip,
        :city      AS city,
        :region    AS region,
        :country   AS country,
        :lat       AS lat,
        :lon       AS lon
    FROM dual
    ) s
    ON (d.geo_id = s.geo_id)
    WHEN MATCHED THEN UPDATE SET
        d.ip        = s.ip,
        d.city      = s.city,
        d.region    = s.region,
        d.country   = s.country,
        d.lat       = s.lat,
        d.lon       = s.lon
    WHEN NOT MATCHED THEN INSERT (
        geo_id, ip, city, region, country, lat, lon
    ) VALUES (
        s.geo_id, s.ip, s.city, s.region, s.country, s.lat, s.lon
    )
    """
    cur.executemany(sql_insert_query,rows)
    print("Data loaded successfully into Oracle!")



    


def main():
    engine = sqlalchemy.create_engine(f"oracle+oracledb://{USER}:{PWD}@localhost:1521/?service_name=XEPDB1")
    with oracledb.connect(user=USER, password=PWD, dsn=DSN) as conn:
        cur = conn.cursor()
        # stg_customer(engine,cur)
        # stg_account(engine,cur)
        # stg_merchant(engine,cur)
        # stg_branches(engine,cur)
        stg_geo(engine,cur)
        conn.commit()

if __name__ == "__main__":
    main()