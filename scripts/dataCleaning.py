# BATCH PROCESSING
"""
BATCH PROCESSING INCLUDES FOR THE FOLLOWING FILES THAT HAS ALREADY BEEN LOADED INTO ORACLE DATABASE
customers - done
accounts 
merchants
branches
geos
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


def stg_customer(engine,cur):
    sql_query = "SELECT * FROM RAW_CUSTOMERS"
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



def main():
    engine = sqlalchemy.create_engine(f"oracle+oracledb://{USER}:{PWD}@localhost:1521/?service_name=XEPDB1")
    with oracledb.connect(user=USER, password=PWD, dsn=DSN) as conn:
        cur = conn.cursor()
        print(cur)
        stg_customer(engine,cur)
        conn.commit()

if __name__ == "__main__":
    main()