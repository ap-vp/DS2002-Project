
PRAGMA foreign_keys = ON;

DROP TABLE IF EXISTS fact_claims;
DROP TABLE IF EXISTS dim_insured;
DROP TABLE IF EXISTS dim_region;
DROP TABLE IF EXISTS dim_date;

-- Date dimension (subset of columns for portability)
CREATE TABLE dim_date (
  date_key INTEGER PRIMARY KEY,
  full_date DATE NOT NULL,
  day_of_week INTEGER,
  day_name TEXT,
  month_of_year INTEGER,
  month_name TEXT,
  calendar_quarter INTEGER,
  calendar_year INTEGER,
  is_weekend INTEGER
);

-- Insured (customer/patient) dimension
CREATE TABLE dim_insured (
  insured_key INTEGER PRIMARY KEY,
  age INTEGER,
  age_band TEXT,
  sex TEXT CHECK (sex IN ('male','female')),
  smoker TEXT CHECK (smoker IN ('yes','no')),
  bmi REAL,
  bmi_category TEXT,
  children INTEGER
);

-- Region/Provider dimension (augmented with providers.json)
CREATE TABLE dim_region (
  region_key INTEGER PRIMARY KEY,
  region TEXT UNIQUE,
  preferred_provider TEXT,
  network_tier TEXT
);

-- Fact table for premium/charges (one row per "claim" or premium record)
CREATE TABLE fact_claims (
  claim_key INTEGER PRIMARY KEY,
  insured_key INTEGER NOT NULL,
  region_key INTEGER NOT NULL,
  date_key INTEGER NOT NULL,
  charges REAL NOT NULL,
  FOREIGN KEY (insured_key) REFERENCES dim_insured(insured_key),
  FOREIGN KEY (region_key) REFERENCES dim_region(region_key),
  FOREIGN KEY (date_key) REFERENCES dim_date(date_key)
);
