

SET FOREIGN_KEY_CHECKS=0;
DROP TABLE IF EXISTS fact_claims;
DROP TABLE IF EXISTS dim_insured;
DROP TABLE IF EXISTS dim_region;
DROP TABLE IF EXISTS dim_date;
SET FOREIGN_KEY_CHECKS=1;

CREATE TABLE dim_date (
  date_key INT PRIMARY KEY,          -- YYYYMMDD
  full_date DATE NOT NULL,
  day_of_week TINYINT,
  day_name VARCHAR(10),
  month_of_year TINYINT,
  month_name VARCHAR(10),
  calendar_quarter TINYINT,
  calendar_year SMALLINT,
  is_weekend TINYINT
) ENGINE=InnoDB;

CREATE TABLE dim_insured (
  insured_key INT AUTO_INCREMENT PRIMARY KEY,
  age TINYINT,
  age_band VARCHAR(10),
  sex ENUM('male','female'),
  smoker ENUM('yes','no'),
  bmi DECIMAL(6,2),
  bmi_category VARCHAR(20),
  children TINYINT
) ENGINE=InnoDB;

CREATE TABLE dim_region (
  region_key INT AUTO_INCREMENT PRIMARY KEY,
  region VARCHAR(20) UNIQUE,
  preferred_provider VARCHAR(64),
  network_tier VARCHAR(16)
) ENGINE=InnoDB;

CREATE TABLE fact_claims (
  claim_key BIGINT AUTO_INCREMENT PRIMARY KEY,
  insured_key INT NOT NULL,
  region_key INT NOT NULL,
  date_key INT NOT NULL,
  charges DECIMAL(12,2) NOT NULL,
  CONSTRAINT fk_fact_insured FOREIGN KEY (insured_key) REFERENCES dim_insured(insured_key),
  CONSTRAINT fk_fact_region  FOREIGN KEY (region_key)  REFERENCES dim_region(region_key),
  CONSTRAINT fk_fact_date    FOREIGN KEY (date_key)    REFERENCES dim_date(date_key)
) ENGINE=InnoDB;

-- Helpful indexes
CREATE INDEX ix_fact_date ON fact_claims(date_key);
CREATE INDEX ix_fact_insured ON fact_claims(insured_key);
CREATE INDEX ix_fact_region ON fact_claims(region_key);