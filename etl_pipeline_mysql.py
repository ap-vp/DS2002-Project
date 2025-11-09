
"""
MySQL ETL variant.
- Loads dimensions and fact into a MySQL database you provision.
- Optional: can enrich region dimension from MongoDB (see MONGO_* settings).

Prereqs:
  pip install sqlalchemy pymysql pandas pymongo

Set env vars or edit constants below before running.

Run:
  python etl/etl_pipeline_mysql.py
"""
import os, json, hashlib
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text

# ---------- CONFIG ----------
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PWD  = os.getenv("MYSQL_PWD", "password")
MYSQL_HOST = os.getenv("MYSQL_HOST", "127.0.0.1")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_DB   = os.getenv("MYSQL_DB", "ds2002_mart")

USE_MONGO_FOR_REGION = os.getenv("USE_MONGO_FOR_REGION", "0") == "1"
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB  = os.getenv("MONGO_DB", "ds2002")
MONGO_COL = os.getenv("MONGO_COL", "providers")

PROJECT_ROOT = os.path.dirname(os.path.dirname(__file__))
DATA_DIR = os.path.join(PROJECT_ROOT, "data")
SQL_DIR  = os.path.join(PROJECT_ROOT, "sql")

def deterministc_date_for_row(row_id:int, year:int=2010)->int:
    rnd = int(hashlib.sha256(str(row_id).encode()).hexdigest(), 16)
    day_of_year = (rnd % 365) + 1
    dt = datetime(year, 1, 1) + timedelta(days=day_of_year - 1)
    return int(dt.strftime("%Y%m%d"))

def age_band(age:int)->str:
    bins = [(0,17,"0-17"),(18,24,"18-24"),(25,34,"25-34"),(35,44,"35-44"),
            (45,54,"45-54"),(55,64,"55-64"),(65,200,"65+")]
    for lo,hi,label in bins:
        if lo <= age <= hi:
            return label
    return "unknown"

def bmi_category(bmi:float)->str:
    if bmi < 18.5: return "Underweight"
    if bmi < 25: return "Normal"
    if bmi < 30: return "Overweight"
    return "Obese"

def make_date_dim(start="2010-01-01", end="2010-12-31"):
    start_dt = datetime.fromisoformat(start)
    end_dt = datetime.fromisoformat(end)
    rows = []
    cur = start_dt
    while cur <= end_dt:
        date_key = int(cur.strftime("%Y%m%d"))
        rows.append({
            "date_key": date_key,
            "full_date": cur.date().isoformat(),
            "day_of_week": cur.isoweekday(),
            "day_name": cur.strftime("%A"),
            "month_of_year": cur.month,
            "month_name": cur.strftime("%B"),
            "calendar_quarter": (cur.month - 1)//3 + 1,
            "calendar_year": cur.year,
            "is_weekend": 1 if cur.weekday() >= 5 else 0,
        })
        cur += timedelta(days=1)
    return pd.DataFrame(rows)

def load_region_from_json():
    providers_path = os.path.join(DATA_DIR, "providers.json")
    with open(providers_path) as f:
        providers_doc = json.load(f)
    return pd.DataFrame(providers_doc["regions"])

def load_region_from_mongo():
    from pymongo import MongoClient
    client = MongoClient(MONGO_URI)
    coll = client[MONGO_DB][MONGO_COL]
    docs = list(coll.find({}, {"_id": 0, "region": 1, "preferred_provider": 1, "network_tier": 1}))
    return pd.DataFrame(docs)

def main():
    # 1) Read insurance CSV
    ins_path = os.path.join(DATA_DIR, "insurance.csv")
    insurance = pd.read_csv(ins_path)
    insurance["age_band"] = insurance["age"].apply(age_band)
    insurance["bmi_category"] = insurance["bmi"].apply(bmi_category)
    insurance["region"] = insurance["region"].str.strip().str.lower()

    # 2) Regions (from Mongo if configured, else JSON)
    if USE_MONGO_FOR_REGION:
        regions = load_region_from_mongo()
    else:
        regions = load_region_from_json()
    # Ensure all present
    regions = insurance[["region"]].drop_duplicates().merge(regions, on="region", how="left").drop_duplicates()
    regions = regions.sort_values("region").reset_index(drop=True)
    regions["region_key"] = regions.index + 1

    # 3) Insured dimension
    insured_dim = insurance[["age","age_band","sex","smoker","bmi","bmi_category","children"]].drop_duplicates().reset_index(drop=True)
    insured_dim["insured_key"] = insured_dim.index + 1

    # 4) Date dimension
    dim_date = make_date_dim("2010-01-01","2010-12-31")

    # 5) Fact
    ins_enriched = insurance.merge(insured_dim, on=["age","age_band","sex","smoker","bmi","bmi_category","children"], how="left")
    ins_enriched = ins_enriched.merge(regions[["region","region_key"]], on="region", how="left")
    ins_enriched["row_id"] = range(1, len(ins_enriched)+1)
    ins_enriched["date_key"] = ins_enriched["row_id"].apply(lambda i: deterministc_date_for_row(i, 2010))
    fact = ins_enriched[["insured_key","region_key","date_key","charges"]].copy()

    # 6) Connect to MySQL
    url = f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PWD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}?charset=utf8mb4"
    engine = create_engine(url, future=True)

    # 7) Create schema (run DDL)
    ddl_path = os.path.join(SQL_DIR, "mysql_create_data_mart.sql")
    with engine.begin() as conn:
        ddl = open(ddl_path, "r").read()
        for stmt in [s.strip() for s in ddl.split(";") if s.strip()]:
            conn.execute(text(stmt))

    # 8) Load dimensions & fact
    dim_date.to_sql("dim_date", engine, if_exists="append", index=False)
    insured_dim[["insured_key","age","age_band","sex","smoker","bmi","bmi_category","children"]].to_sql("dim_insured", engine, if_exists="append", index=False)
    regions[["region_key","region","preferred_provider","network_tier"]].to_sql("dim_region", engine, if_exists="append", index=False)
    fact.to_sql("fact_claims", engine, if_exists="append", index=False)

    print("Load complete. Try the queries in sql/mysql_sample_queries.sql")

if __name__ == "__main__":
    main()
