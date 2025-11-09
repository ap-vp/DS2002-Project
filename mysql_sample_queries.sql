-- Sample analytics for MySQL
-- 1) Total charges by smoker and region per year
SELECT d.calendar_year,
       i.smoker,
       r.region,
       ROUND(SUM(f.charges), 2) AS total_charges
FROM fact_claims f
JOIN dim_insured i ON f.insured_key = i.insured_key
JOIN dim_region r  ON f.region_key = r.region_key
JOIN dim_date d    ON f.date_key   = d.date_key
GROUP BY d.calendar_year, i.smoker, r.region
ORDER BY d.calendar_year, i.smoker, r.region;

-- 2) Average charge by BMI category and age band (top 10)
SELECT i.bmi_category,
       i.age_band,
       ROUND(AVG(f.charges), 2) AS avg_charge,
       COUNT(*) AS records
FROM fact_claims f
JOIN dim_insured i ON f.insured_key = i.insured_key
GROUP BY i.bmi_category, i.age_band
ORDER BY avg_charge DESC
LIMIT 10;

-- 3) Share of yearly charges by smoker status
WITH totals AS (
  SELECT d.calendar_year AS y, SUM(f.charges) AS year_total
  FROM fact_claims f JOIN dim_date d ON f.date_key = d.date_key
  GROUP BY d.calendar_year
)
SELECT d.calendar_year,
       i.smoker,
       ROUND(100 * SUM(f.charges)/t.year_total, 2) AS pct_of_year
FROM fact_claims f
JOIN dim_insured i ON f.insured_key = i.insured_key
JOIN dim_date d ON f.date_key = d.date_key
JOIN totals t ON t.y = d.calendar_year
GROUP BY d.calendar_year, i.smoker
ORDER BY d.calendar_year, i.smoker;