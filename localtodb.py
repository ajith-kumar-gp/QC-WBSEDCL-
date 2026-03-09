import pandas as pd
import psycopg2
import hashlib
from psycopg2.extras import execute_batch

DB_CONFIG = {
    "host": "wbpdclmaster.c07osc0km7kz.us-east-1.rds.amazonaws.com",
    "port": 5432,
    "database": "wbpdcl",
    "user": "postgres",
    "password": "Sujanix#123",
}

def safe(val):
    """Convert pandas NaN to None"""
    if pd.isna(val):
        return None
    return val

print("Connecting to database...")
conn = psycopg2.connect(**DB_CONFIG)
cur = conn.cursor()

print("Loading Excel...")
df = pd.read_excel("data.xlsx", engine="openpyxl")

# Normalize column names
df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

# Remove duplicate columns
df = df.loc[:, ~df.columns.duplicated()]

print("Columns detected:")
print(df.columns)

# Convert PS_DT (DD.MM.YYYY)
if "ps_dt" in df.columns:
    df["ps_dt"] = pd.to_datetime(df["ps_dt"], format="%d.%m.%Y", errors="coerce")

# Fix timestamps
if "created_at" in df.columns:
    df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce", utc=True).dt.tz_localize(None)

if "updated_at" in df.columns:
    df["updated_at"] = pd.to_datetime(df["updated_at"], errors="coerce", utc=True).dt.tz_localize(None)

print("Grouping by agency...")

records = []

for agency, group in df.groupby("agency"):

    # sample 150 rows per CCC
    sample = group.sample(n=min(150, len(group)), random_state=42)

    for _, row in sample.iterrows():

        raw = f"{row.get('consumer_id')}|{row.get('img_url')}"
        row_id = hashlib.md5(raw.encode()).hexdigest()

        records.append((
            row_id,
            safe(row.get("consumer_id")),
            row["ps_dt"].date() if pd.notna(row.get("ps_dt")) else None,
            safe(row.get("actual_reading")),
            safe(row.get("ai_meter_reading")),
            float(row.get("confidence_score")) if pd.notna(row.get("confidence_score")) else None,
            safe(row.get("agency")),
            safe(row.get("division_name")),
            safe(row.get("vendor")),
            safe(row.get("img_url")),
            safe(row.get("img_status")),
            safe(row.get("meter_status")),
            safe(row.get("ocr_status")),
            row["created_at"].to_pydatetime() if pd.notna(row.get("created_at")) else None,
            row["updated_at"].to_pydatetime() if pd.notna(row.get("updated_at")) else None,
        ))

print(f"Inserting {len(records)} rows into DB...")

execute_batch(cur, """
INSERT INTO meter_images (
    row_id,
    consumer_id,
    ps_dt,
    actual_reading,
    ai_meter_reading,
    confidence_score,
    agency,
    division_name,
    vendor,
    img_url,
    img_status,
    meter_status,
    ocr_status,
    created_at,
    updated_at
)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
ON CONFLICT (row_id) DO NOTHING
""", records, page_size=500)

conn.commit()

cur.close()
conn.close()

print("✅ Import complete.")