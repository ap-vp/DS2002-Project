
"""
ETL pipeline for DS-2002 Insurance Data Mart

- Extracts:
    1) File system: data/insurance.csv
    2) Semi-structured "NoSQL-like": data/providers.json
    3) Relational SQL: executes classicmodels SQL to a local SQLite DB (sources/classicmodels.db) and
       extracts a tiny reference table (offices) just to prove SQL extraction. 

- Transforms:
    * Derives age bands and BMI categories
    * Normalizes region names
    * Generates a reproducible synthetic claim_date to satisfy the Date dimension requirement
    * Reduces/reshapes columns (modifies number of columns from source to destination)

- Loads:
    * Populates star schema tables in a local SQLite database: data_mart.db
    * Runs sample analytic queries from sql/sample_queries.sql

Run:
    python etl/etl_pipeline.py
"""
import os, json, sqlite3, random, hashlib
import pandas as pd
from datetime import datetime, timedelta

PROJECT_ROOT = os.path.dirname(os.path.dirname(__file__))
DATA_DIR = os.path.join(PROJECT_ROOT, "data")
SQL_DIR = os.path.join(PROJECT_ROOT, "sql")
SOURCES_DIR = os.path.join(PROJECT_ROOT, "sources")
MART_DB = os.path.join(PROJECT_ROOT, "data_mart.db")
CLASSICMODELS_DB = os.path.join(SOURCES_DIR, "classicmodels.db")

def exec_sql_script(conn: sqlite3.Connection, path: str):
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        sql = f.read()
    conn.executescript(sql)

def build_classicmodels_sqlite():
    """Load provided MySQL sample database into a local SQLite DB (best effort)."""
    sql_path = os.path.join(SOURCES_DIR, "mysqlsampledatabase.sql")
    if not os.path.exists(sql_path):
        print("classicmodels SQL not found; skipping SQL extraction step.")
        return
    conn = sqlite3.connect(CLASSICMODELS_DB)
    conn.execute("PRAGMA foreign_keys = ON;")
    try:
        exec_sql_script(conn, sql_path)
        conn.commit()
        # Quick proof of extraction: create a tiny lookup with office territories
        try:
            df = pd.read_sql_query("SELECT officeCode, city, country, territory FROM offices;", conn)
            df.to_csv(os.path.join(DATA_DIR, "classicmodels_offices_extract.csv"), index=False)
            print(f"Extracted {len(df)} office rows from SQL source.")
        except Exception as e:
            print("Extraction query failed (likely due to SQLite type quirks), continuing. Error:", e)
    finally:
        conn.close()

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

def deterministc_date_for_row(row_id:int, year:int=2010)->int:
    # Stable date per row using a hash; spreads across the year
    rnd = int(hashlib.sha256(str(row_id).encode()).hexdigest(), 16)
    day_of_year = (rnd % 365) + 1
    dt = datetime(year, 1, 1) + timedelta(days=day_of_year - 1)
    return int(dt.strftime("%Y%m%d"))

def main():
    # 1) Build a local classicmodels.db from the provided SQL (relational source)
    build_classicmodels_sqlite()

    # 2) Read file sources
    ins_path = os.path.join(DATA_DIR, "insurance.csv")
    providers_path = os.path.join(DATA_DIR, "providers.json")
    insurance = pd.read_csv(ins_path)
    with open(providers_path) as f:
        providers_doc = json.load(f)
    providers = pd.DataFrame(providers_doc["regions"])

    # 3) Transform insurance to dimensions
    insured = insurance.copy()
    insured["age_band"] = insured["age"].apply(age_band)
    insured["bmi_category"] = insured["bmi"].apply(bmi_category)

    # Normalize region strings
    insured["region"] = insured["region"].str.strip().str.lower()

    # 4) Build region dim (left-join to providers.json to demonstrate semi-structured enrichment)
    regions = insured[["region"]].drop_duplicates().merge(providers, on="region", how="left")
    regions = regions.sort_values("region").reset_index(drop=True)
    regions["region_key"] = regions.index + 1

    # 5) Build insured dim with surrogate keys
    insured_dim = insured[["age","age_band","sex","smoker","bmi","bmi_category","children"]].drop_duplicates().reset_index(drop=True)
    insured_dim["insured_key"] = insured_dim.index + 1

    # 6) Build date dimension
    dim_date = make_date_dim("2010-01-01","2010-12-31")

    # 7) Build fact table: assign insured_key & region_key; synthetic date for requirement
    ins_enriched = insured.merge(insured_dim, on=["age","age_band","sex","smoker","bmi","bmi_category","children"], how="left")
    ins_enriched = ins_enriched.merge(regions[["region","region_key"]], on="region", how="left")

    ins_enriched["row_id"] = np.arange(1, len(ins_enriched)+1)
    ins_enriched["date_key"] = ins_enriched["row_id"].apply(lambda i: deterministc_date_for_row(i, 2010))

    fact = ins_enriched[["insured_key","region_key","date_key","charges"]].copy()
    fact["claim_key"] = np.arange(1, len(fact)+1)

    # 8) Load into SQLite mart
    if os.path.exists(MART_DB): os.remove(MART_DB)
    conn = sqlite3.connect(MART_DB)
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.executescript(open(os.path.join(SQL_DIR, "create_data_mart.sql")).read())
    dim_date.to_sql("dim_date", conn, if_exists="append", index=False)
    insured_dim[["insured_key","age","age_band","sex","smoker","bmi","bmi_category","children"]].to_sql("dim_insured", conn, if_exists="append", index=False)
    regions[["region_key","region","preferred_provider","network_tier"]].to_sql("dim_region", conn, if_exists="append", index=False)
    fact[["claim_key","insured_key","region_key","date_key","charges"]].to_sql("fact_claims", conn, if_exists="append", index=False)

    # 9) Run sample queries to verify
    print("Row counts:")
    for tbl in ["dim_date","dim_insured","dim_region","fact_claims"]:
        cur = conn.execute(f"SELECT COUNT(*) FROM {tbl}")
        print(f"  {tbl}: {cur.fetchone()[0]}")

    print("\nSample query: total charges by smoker & region in 2010 (top 5 rows):")
    q1 = """
    SELECT d.calendar_year, i.smoker, r.region, ROUND(SUM(f.charges),2) AS total_charges
    FROM fact_claims f
    JOIN dim_insured i ON f.insured_key = i.insured_key
    JOIN dim_region r  ON f.region_key = r.region_key
    JOIN dim_date d    ON f.date_key   = d.date_key
    GROUP BY d.calendar_year, i.smoker, r.region
    ORDER BY total_charges DESC
    LIMIT 5;
    """
    print(pd.read_sql_query(q1, conn))

    conn.close()
    print(f"\nAll done. SQLite mart at: {MART_DB}")

if __name__ == "__main__":
    main()
