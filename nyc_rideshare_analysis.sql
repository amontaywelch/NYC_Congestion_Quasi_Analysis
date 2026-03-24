-- ============================================================
-- NYC Rideshare Congestion Pricing Analysis
-- Quasi-experimental before/after analysis of NYC congestion 
-- pricing policy impact on rideshare behavior (Oct-Dec 2024 vs Feb-Apr 2025)
-- January 2025 marked the congestion fee's implementation. 
-- ============================================================


-- joining tables together to view row counts (2024)
SELECT 'oct_2024' as month, COUNT(*) as total_trips FROM `project-715687c4-6754-4729-834.nyc_rideshare.rideshare_2024_10`
UNION ALL
SELECT 'nov_2024', COUNT(*) FROM `project-715687c4-6754-4729-834.nyc_rideshare.rideshare_2024_11`
UNION ALL
SELECT 'dec_2024', COUNT(*) FROM `project-715687c4-6754-4729-834.nyc_rideshare.rideshare_2024_12`;


-- joining tables together to view row counts (2025), January excluded as implementation month
SELECT 'feb_2025' as month, COUNT(*) as total_trips FROM `project-715687c4-6754-4729-834.nyc_rideshare.rideshare_2025_02`
UNION ALL
SELECT 'mar_2025' as month, COUNT(*) FROM `project-715687c4-6754-4729-834.nyc_rideshare.rideshare_2025_03`
UNION ALL
SELECT 'apr_2025' as month, COUNT(*) FROM `project-715687c4-6754-4729-834.nyc_rideshare.rideshare_2025_04`;


-- checking column schemas, one month per year selected as schemas are consistent within each year
SELECT column_name, data_type
FROM `project-715687c4-6754-4729-834.nyc_rideshare.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'rideshare_2025_02'
ORDER BY ordinal_position;

SELECT column_name, data_type
FROM `project-715687c4-6754-4729-834.nyc_rideshare.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'rideshare_2024_12'
ORDER BY ordinal_position;


-- combining all monthly tables into one centralized table, adding period label and null cbd_congestion_fee for 2024
CREATE OR REPLACE TABLE `project-715687c4-6754-4729-834.nyc_rideshare.rideshare_all` AS

SELECT 
  pickup_datetime,
  dropoff_datetime,
  PULocationID,
  DOLocationID,
  trip_miles,
  trip_time,
  base_passenger_fare,
  tolls,
  tips,
  congestion_surcharge,
  shared_request_flag,
  shared_match_flag,
  'before' as period,
  NULL as cbd_congestion_fee
FROM `project-715687c4-6754-4729-834.nyc_rideshare.rideshare_2024_10`

UNION ALL

SELECT 
  pickup_datetime,
  dropoff_datetime,
  PULocationID,
  DOLocationID,
  trip_miles,
  trip_time,
  base_passenger_fare,
  tolls,
  tips,
  congestion_surcharge,
  shared_request_flag,
  shared_match_flag,
  'before' as period,
  NULL as cbd_congestion_fee
FROM `project-715687c4-6754-4729-834.nyc_rideshare.rideshare_2024_11`

UNION ALL

SELECT 
  pickup_datetime,
  dropoff_datetime,
  PULocationID,
  DOLocationID,
  trip_miles,
  trip_time,
  base_passenger_fare,
  tolls,
  tips,
  congestion_surcharge,
  shared_request_flag,
  shared_match_flag,
  'before' as period,
  NULL as cbd_congestion_fee
FROM `project-715687c4-6754-4729-834.nyc_rideshare.rideshare_2024_12`

UNION ALL

SELECT 
  pickup_datetime,
  dropoff_datetime,
  PULocationID,
  DOLocationID,
  trip_miles,
  trip_time,
  base_passenger_fare,
  tolls,
  tips,
  congestion_surcharge,
  shared_request_flag,
  shared_match_flag,
  'after' as period,
  cbd_congestion_fee
FROM `project-715687c4-6754-4729-834.nyc_rideshare.rideshare_2025_02`

UNION ALL

SELECT 
  pickup_datetime,
  dropoff_datetime,
  PULocationID,
  DOLocationID,
  trip_miles,
  trip_time,
  base_passenger_fare,
  tolls,
  tips,
  congestion_surcharge,
  shared_request_flag,
  shared_match_flag,
  'after' as period,
  cbd_congestion_fee
FROM `project-715687c4-6754-4729-834.nyc_rideshare.rideshare_2025_03`

UNION ALL

SELECT 
  pickup_datetime,
  dropoff_datetime,
  PULocationID,
  DOLocationID,
  trip_miles,
  trip_time,
  base_passenger_fare,
  tolls,
  tips,
  congestion_surcharge,
  shared_request_flag,
  shared_match_flag,
  'after' as period,
  cbd_congestion_fee
FROM `project-715687c4-6754-4729-834.nyc_rideshare.rideshare_2025_04`;


-- data quality checks to identify nulls, zero values, negative values, and outliers before cleaning
SELECT
  COUNT(*) as total_rows,
  COUNTIF(pickup_datetime IS NULL) as null_pickup_datetime,
  COUNTIF(dropoff_datetime IS NULL) as null_dropoff_datetime,
  COUNTIF(PULocationID IS NULL) as null_pickup_location,
  COUNTIF(DOLocationID IS NULL) as null_dropoff_location,
  COUNTIF(trip_miles IS NULL) as null_trip_miles,
  COUNTIF(trip_time IS NULL) as null_trip_time,
  COUNTIF(base_passenger_fare IS NULL) as null_fare,
  COUNTIF(tips IS NULL) as null_tips,
  COUNTIF(trip_miles = 0) as zero_miles,
  COUNTIF(trip_time = 0) as zero_time,
  COUNTIF(base_passenger_fare = 0) as zero_fare,
  COUNTIF(base_passenger_fare < 0) as negative_fare,
  COUNTIF(trip_miles < 0) as negative_miles,
  COUNTIF(trip_time < 0) as negative_time,
  COUNTIF(trip_miles > 100) as trips_over_100_miles,
  COUNTIF(trip_time > 7200) as trips_over_2_hours,
  COUNTIF(base_passenger_fare > 500) as fares_over_500,
  ROUND(MIN(trip_miles), 2) as min_miles,
  ROUND(MAX(trip_miles), 2) as max_miles,
  ROUND(MIN(base_passenger_fare), 2) as min_fare,
  ROUND(MAX(base_passenger_fare), 2) as max_fare,
  MIN(pickup_datetime) as earliest_trip,
  MAX(pickup_datetime) as latest_trip
FROM `project-715687c4-6754-4729-834.nyc_rideshare.rideshare_all`;


-- checking for duplicate records across key trip identifier columns
SELECT
  pickup_datetime,
  dropoff_datetime,
  PULocationID,
  DOLocationID,
  trip_miles,
  COUNT(*) as duplicate_count
FROM `project-715687c4-6754-4729-834.nyc_rideshare.rideshare_all`
GROUP BY
  pickup_datetime,
  dropoff_datetime,
  PULocationID,
  DOLocationID,
  trip_miles
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC;


-- creating clean table by removing invalid records and duplicates in a single pass
CREATE OR REPLACE TABLE `project-715687c4-6754-4729-834.nyc_rideshare.rideshare_clean` AS

SELECT DISTINCT *
FROM `project-715687c4-6754-4729-834.nyc_rideshare.rideshare_all`
WHERE
  trip_miles > 0
  AND trip_miles IS NOT NULL
  AND trip_time > 0
  AND trip_time IS NOT NULL
  AND base_passenger_fare > 0
  AND base_passenger_fare <= 500
  AND base_passenger_fare >= 0
  AND trip_miles <= 100
  AND PULocationID IS NOT NULL
  AND DOLocationID IS NOT NULL
  AND pickup_datetime IS NOT NULL
  AND dropoff_datetime IS NOT NULL;


-- validating cleaning impact, target is less than 5% rows removed
SELECT
  COUNT(*) as clean_rows,
  (SELECT COUNT(*) FROM `project-715687c4-6754-4729-834.nyc_rideshare.rideshare_all`) as original_rows,
  (SELECT COUNT(*) FROM `project-715687c4-6754-4729-834.nyc_rideshare.rideshare_all`) - COUNT(*) as rows_removed,
  ROUND(((SELECT COUNT(*) FROM `project-715687c4-6754-4729-834.nyc_rideshare.rideshare_all`) - COUNT(*)) / 
  (SELECT COUNT(*) FROM `project-715687c4-6754-4729-834.nyc_rideshare.rideshare_all`) * 100, 2) as pct_removed
FROM `project-715687c4-6754-4729-834.nyc_rideshare.rideshare_clean`;


-- time of day analysis to identify if congestion pricing shifted ridership patterns by hour
SELECT
  period,
  EXTRACT(HOUR FROM pickup_datetime) as hour_of_day,
  COUNT(*) as total_trips,
  ROUND(AVG(base_passenger_fare), 2) as avg_fare,
  ROUND(AVG(trip_time / 60), 2) as avg_minutes,
  ROUND(AVG(trip_miles), 2) as avg_miles
FROM `project-715687c4-6754-4729-834.nyc_rideshare.rideshare_clean`
GROUP BY period, hour_of_day
ORDER BY hour_of_day, period DESC;


-- congestion fee breakdown showing charge rate, average fee, and total revenue collected
SELECT
  COUNTIF(cbd_congestion_fee > 0) as trips_charged_fee,
  COUNTIF(cbd_congestion_fee = 0) as trips_not_charged,
  COUNT(*) as total_after_trips,
  ROUND(COUNTIF(cbd_congestion_fee > 0) / COUNT(*) * 100, 2) as pct_trips_charged,
  ROUND(AVG(CASE WHEN cbd_congestion_fee > 0 THEN cbd_congestion_fee END), 2) as avg_fee_when_charged,
  ROUND(MIN(CASE WHEN cbd_congestion_fee > 0 THEN cbd_congestion_fee END), 2) as min_fee,
  ROUND(MAX(cbd_congestion_fee), 2) as max_fee,
  ROUND(SUM(cbd_congestion_fee), 2) as total_fees_collected
FROM `project-715687c4-6754-4729-834.nyc_rideshare.rideshare_clean`
WHERE period = 'after';


-- overall before vs after comparison across key trip metrics
SELECT
  period,
  COUNT(*) as total_trips,
  ROUND(AVG(trip_miles), 2) as avg_trip_miles,
  ROUND(AVG(trip_time / 60), 2) as avg_trip_minutes,
  ROUND(AVG(base_passenger_fare), 2) as avg_fare,
  ROUND(AVG(tips), 2) as avg_tip,
  ROUND(AVG(tips / NULLIF(base_passenger_fare, 0)) * 100, 2) as avg_tip_pct,
  ROUND(AVG(CASE WHEN shared_request_flag = 'Y' THEN 1 ELSE 0 END) * 100, 2) as shared_ride_pct
FROM `project-715687c4-6754-4729-834.nyc_rideshare.rideshare_clean`
GROUP BY period
ORDER BY period DESC;


-- Manhattan-specific trip metrics, isolated as the primary congestion zone
SELECT
  period,
  COUNT(*) as total_trips,
  ROUND(AVG(base_passenger_fare), 2) as avg_fare,
  ROUND(AVG(tips), 2) as avg_tip,
  ROUND(AVG(trip_miles), 2) as avg_miles,
  ROUND(AVG(trip_time / 60), 2) as avg_minutes
FROM `project-715687c4-6754-4729-834.nyc_rideshare.rideshare_clean`
WHERE DOLocationID IN (
  SELECT LocationID 
  FROM `project-715687c4-6754-4729-834.nyc_rideshare.taxi_zones`
  WHERE zone LIKE '%Manhattan%'
  OR borough = 'Manhattan'
)
GROUP BY period
ORDER BY period DESC;


-- top 25 dropoff zones by trip volume with avg fare and duration, before vs after
SELECT
  z.zone,
  z.borough,
  period,
  COUNT(*) as total_trips,
  ROUND(AVG(base_passenger_fare), 2) as avg_fare,
  ROUND(AVG(trip_time / 60), 2) as avg_minutes
FROM `project-715687c4-6754-4729-834.nyc_rideshare.rideshare_clean` AS r
JOIN `project-715687c4-6754-4729-834.nyc_rideshare.taxi_zones` AS z
  ON r.DOLocationID = z.LocationID
GROUP BY z.zone, z.borough, period
ORDER BY total_trips DESC
LIMIT 25;


-- top 25 zones ranked by largest trip volume decline after congestion pricing
WITH before_data AS (
  SELECT
    z.zone,
    z.borough,
    COUNT(*) as before_trips,
    ROUND(AVG(base_passenger_fare), 2) as before_fare,
    ROUND(AVG(trip_time / 60), 2) as before_minutes
  FROM `project-715687c4-6754-4729-834.nyc_rideshare.rideshare_clean` r
  JOIN `project-715687c4-6754-4729-834.nyc_rideshare.taxi_zones` z
    ON r.DOLocationID = z.LocationID
  WHERE period = 'before'
  GROUP BY z.zone, z.borough
),
after_data AS (
  SELECT
    z.zone,
    z.borough,
    COUNT(*) as after_trips,
    ROUND(AVG(base_passenger_fare), 2) as after_fare,
    ROUND(AVG(trip_time / 60), 2) as after_minutes
  FROM `project-715687c4-6754-4729-834.nyc_rideshare.rideshare_clean` r
  JOIN `project-715687c4-6754-4729-834.nyc_rideshare.taxi_zones` z
    ON r.DOLocationID = z.LocationID
  WHERE period = 'after'
  GROUP BY z.zone, z.borough
)
SELECT
  b.zone,
  b.borough,
  b.before_trips,
  a.after_trips,
  ROUND((a.after_trips - b.before_trips) / b.before_trips * 100, 2) as trip_change_pct,
  b.before_minutes,
  a.after_minutes,
  ROUND(a.after_minutes - b.before_minutes, 2) as minutes_change
FROM before_data b
JOIN after_data a ON b.zone = a.zone
ORDER BY trip_change_pct ASC
LIMIT 25;


-- borough level summary of trip volume, fare, duration and distance before vs after
SELECT
  z.borough,
  period,
  COUNT(*) as total_trips,
  ROUND(AVG(base_passenger_fare), 2) as avg_fare,
  ROUND(AVG(trip_time / 60), 2) as avg_minutes,
  ROUND(AVG(trip_miles), 2) as avg_miles
FROM `project-715687c4-6754-4729-834.nyc_rideshare.rideshare_clean` r
JOIN `project-715687c4-6754-4729-834.nyc_rideshare.taxi_zones` z
  ON r.DOLocationID = z.LocationID
WHERE z.borough NOT IN ('N/A', 'Unknown')
GROUP BY z.borough, period
ORDER BY borough, period DESC;


-- shared ride percentage by borough before vs after, excluding non-NYC zones
SELECT
  z.borough,
  period,
  ROUND(AVG(CASE WHEN shared_request_flag = 'Y' THEN 1 ELSE 0 END) * 100, 2) as shared_ride_pct,
  COUNT(*) as total_trips
FROM `project-715687c4-6754-4729-834.nyc_rideshare.rideshare_clean` r
JOIN `project-715687c4-6754-4729-834.nyc_rideshare.taxi_zones` z
  ON r.DOLocationID = z.LocationID
WHERE z.borough NOT IN ('N/A', 'EWR', 'Unknown')
GROUP BY z.borough, period
ORDER BY borough, period DESC;


-- monthly trip volume and key metrics trend across the full analysis period
SELECT
  FORMAT_DATE('%Y-%m', DATE(pickup_datetime)) as month,
  period,
  COUNT(*) as total_trips,
  ROUND(AVG(base_passenger_fare), 2) as avg_fare,
  ROUND(AVG(trip_time / 60), 2) as avg_minutes
FROM `project-715687c4-6754-4729-834.nyc_rideshare.rideshare_clean`
GROUP BY month, period
ORDER BY month;
