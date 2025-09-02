AML RAW Dataset (US-based)
Synthetic, intentionally messy CSVs for ETL practice. Mixed date formats, whitespace/casing noise, occasional nulls and duplicates.
Files:
- customers_raw.csv: customer_id, name, dob, kyc_status, email, phone, address, city, state, zip, country
- accounts_raw.csv: account_id, customer_id, type, currency, balance, status, opened_at, branch_id
- merchants_raw.csv: merchant_id, name, mcc, category, city, state, country_code
- devices_raw.csv: device_id, fingerprint, os, model
- geos_raw.csv: geo_id, ip, city, region, country, lat, lon
- branches_raw.csv: branch_id, name, city, state, country
- sanctions_raw.csv: sanction_id, list_name, entity_name, risk_level
- logins_raw.csv: login_id, customer_id, device_id, geo_id, ts, channel, result
- transactions_raw.csv: txn_id, src_account_id, dst_account_id, merchant_id, amount, currency, channel, ts, status
Currency: USD. Locations: US cities/states.
