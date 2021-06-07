## Create database

!hdfs dfs -mkdir /hive ;

CREATE DATABASE cars_db;

SHOW DATABASES;

USE cars_db;


--Create table schema
CREATE EXTERNAL TABLE IF NOT EXISTS cars(
  maker STRING,
  model STRING,
  mileage INT,
  manufacture_year INT,
  engine_displacement INT,
  engine_power INT,
  body_type STRING,
  color_slug STRING,
  stk_year INT,
  transmission STRING,
  door_count INT,
  seat_count INT,
  fuel_type STRING,
  date_created DATE,
  date_last_seen DATE,
  price_eur FLOAT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
tblproperties("skip.header.line.count"="1");


!hdfs dfs -copyFromLocal cars.csv /hive/ ;

--Load data into table
LOAD DATA INPATH '/hive/cars.csv' INTO TABLE cars;

set hive.cli.print.header=true;
set hive.resultset.use.unique.column.names=false;

--check if data loaded correctly
SELECT * FROM cars LIMIT 10;

--Total row count
SELECT COUNT(*) FROM cars;


# Some exploratory Analysis

--Top 10 most expensive cars
SELECT maker, model, mileage, price_eur
FROM cars
ORDER BY price_eur DESC
LIMIT 10 ;






-- Top 10 popular car makers
SELECT maker, COUNT(maker) AS count
FROM cars
GROUP BY maker
ORDER BY count DESC
LIMIT 10;

-- unique makers
SELECT COUNT(DISTINCT maker)
FROM cars;

#47

-- How many unique models
SELECT COUNT(DISTINCT model)
FROM cars;
#1013

-- Top 10 popular car models
SELECT model, COUNT(model) AS model_count
FROM cars
GROUP BY model
ORDER BY model_count DESC
LIMIT 10;



-- Check different fuel types and their counts
SELECT fuel_type , COUNT(fuel_type) AS count
FROM cars
GROUP BY fuel_type;


-- Check door_count and their counts
SELECT door_count , COUNT(door_count) AS count
FROM cars
GROUP BY door_count
ORDER BY count DESC;


## Most of cars are 4 doored as one would expect.

-- Check seat_count and their counts
SELECT seat_count , COUNT(seat_count) AS count
FROM cars
GROUP BY seat_count
ORDER BY count DESC;

## Most of cars have 5 seats i.e., typical in a 4 door sedan

--- manufacturing year and ther count
SELECT manufacture_year, COUNT(manufacture_year) AS count
FROM cars
GROUP BY  manufacture_year
ORDER BY manufacture_year DESC
LIMIT 300;

# It is highly unlikely to have manufacturing years earlier than 1700, because
# cars were invented in 18th century. Earlier records are hard to explain.
# Most of used cars are from last two decades.
# 2017 shows fewer records than earlier years indicating incomplete records.

-- sticker years and their counts
SELECT stk_year, COUNT(stk_year) AS count
FROM cars
GROUP BY  stk_year
ORDER BY stk_year ASC
LIMIT 20;

#Unreasonable values for sticker year

--transmission types and their counts
SELECT transmission, COUNT(*) AS trsm_count
FROM cars
GROUP BY  transmission
ORDER BY trsm_count;

#manual transmission is most common type of transmission

###################################################

## Descriptive Statistics

-- mileage
SELECT
ROUND(MIN(mileage),2) AS min_mileage,
ROUND(MAX(mileage),2) AS max_mileage,
ROUND(AVG(mileage),2) AS avg_mileage,
ROUND(STDDEV_POP(mileage),2) AS std_mileage
FROM cars;


-- enginer power
SELECT
ROUND(MIN(engine_power),2) AS min_engine_power,
ROUND(MAX(engine_power),2) AS max_engine_power,
ROUND(AVG(engine_power),2) AS avg_engine_power
FROM cars;

-- engine displacement
SELECT
ROUND(MIN(engine_displacement),2) AS min_eng_displacement,
ROUND(MAX(engine_displacement),2) AS max_eng_displacement,
ROUND(AVG(engine_displacement),2) AS avg_eng_displacement
FROM cars;


-- price
SELECT
ROUND(MIN(price_eur),2) AS min_pirce,
ROUND(MAX(price_eur),2) AS max_price,
ROUND(AVG(price_eur),2) AS avg_price
FROM cars;







##################################################
-- Count Percent Null values in some columns

SELECT
round(100.0 * SUM(CASE WHEN maker = '' THEN 1 ELSE 0 END) / COUNT(*),2) AS maker_pct_null,
round(100.0 * SUM(CASE WHEN model = '' THEN 1 ELSE 0 END) / COUNT(*),2) AS model_pct_null,
round(100.0 * SUM(CASE WHEN mileage IS NULL THEN 1 ELSE 0 END) / COUNT(*),2) AS mlg_pct_null,
round(100.0 * SUM(CASE WHEN manufacture_year IS NULL THEN 1 ELSE 0 END) / COUNT(*),2) AS mfc_yr_pct_null,
round(100.0 * SUM(CASE WHEN stk_year IS NULL THEN 1 ELSE 0 END) / COUNT(*),2) AS stk_yr_pct_null,
round(100.0 * SUM(CASE WHEN engine_displacement IS NULL THEN 1 ELSE 0 END) / COUNT(*),2) AS engine_disc_pct_null,
round(100.0 * SUM(CASE WHEN engine_power IS NULL THEN 1 ELSE 0 END) / COUNT(*),2) AS engine_pwr_pct_null,
round(100.0 * SUM(CASE WHEN body_type = '' THEN 1 ELSE 0 END) / COUNT(*),2) AS body_typ_pct_null,
round(100.0 * SUM(CASE WHEN color_slug = '' THEN 1 ELSE 0 END) / COUNT(*),2) AS col_slg_pct_null,
round(100.0 * SUM(CASE WHEN door_count IS NULL THEN 1 ELSE 0 END) / COUNT(*),2) AS door_cnt_pct_null,
round(100.0 * SUM(CASE WHEN price_eur IS NULL THEN 1 ELSE 0 END) / COUNT(*),2) AS price_pct_null
FROM cars;

###########################





--Create new clean Table
CREATE TABLE IF NOT EXISTS clean_cars AS
SELECT maker, model, mileage, manufacture_year, transmission, door_count, seat_count, fuel_type, date_created, date_last_seen, price_eur
FROM cars
WHERE model != ''
AND mileage BETWEEN '5000' AND '100000'
AND manufacture_year BETWEEN '2007' AND '2017'
AND price_eur BETWEEN '5000' AND '200000'
ORDER BY maker, model;


-- data preview
SELECT * FROM clean_cars
LIMIT 5;



## Descriptive Statistics

-- mileage
SELECT
ROUND(AVG(mileage),2) AS avg_mileage,
ROUND(STDDEV_POP(mileage),2) AS std_mileage
FROM clean_cars;




-- price
SELECT
ROUND(AVG(price_eur),2) AS avg_price
FROM clean_cars;



# Some exploratory Analysis on clean_cars

--Top 10 most expensive cars
SELECT maker, model, mileage, price_eur
FROM clean_cars
ORDER BY price_eur DESC
LIMIT 10 ;






-- Top 10 popular car makers
SELECT maker, COUNT(maker) AS count
FROM clean_cars
GROUP BY maker
ORDER BY count DESC
LIMIT 10;

-- unique makers
SELECT COUNT(DISTINCT maker)
FROM clean_cars;
#47

-- How many unique models
SELECT COUNT(DISTINCT model)
FROM clean_cars;
#1013

-- Top 25 available car models
SELECT maker, model, COUNT(model) AS count, ROUND(AVG(price_eur),0) as avg_price, door_count
FROM clean_cars
GROUP BY maker, model, door_count
ORDER BY  count DESC, avg_price DESC
LIMIT 25;


--- cars which have driven least i.e. 5000 km and are also cheaper
SELECT maker, model, COUNT(model) AS count, ROUND(AVG(price_eur),0) as avg_price, manufacture_year
FROM clean_cars
WHERE mileage = 5000
GROUP BY maker, model, door_count, manufacture_year
ORDER BY  avg_price ASC
LIMIT 25;






-- Check different fuel types and their counts
SELECT fuel_type , COUNT(fuel_type) AS count
FROM clean_cars
GROUP BY fuel_type
ORDER BY count DESC;


-- Check door_count and their counts
SELECT door_count , COUNT(door_count) AS count
FROM clean_cars
GROUP BY door_count
ORDER BY count DESC;


## Most of cars are 4 doored as one would expect.

-- Check seat_count and their counts
SELECT seat_count , COUNT(seat_count) AS count
FROM clean_cars
WHERE seat_count BETWEEN '3' and '8'
GROUP BY seat_count
ORDER BY count DESC;

## Most of cars have 5 seats i.e., typical in a 4 door sedan

--- manufacturing year and ther count
SELECT manufacture_year, COUNT(manufacture_year) AS count
FROM clean_cars
GROUP BY  manufacture_year
ORDER BY manufacture_year DESC;

# It is highly unlikely to have manufacturing years earlier than 1700, because
# cars were invented in 18th century. Earlier records are hard to explain.
# Most of used cars are from last two decades.
# 2017 shows fewer records than earlier years indicating incomplete records.


--transmission types and their counts
SELECT transmission, COUNT(*) AS trsm_count
FROM clean_cars
GROUP BY  transmission
ORDER BY trsm_count DESC;


SELECT date_created
FROM clean_cars
LIMIT 5;

SELECT
maker, model, price_eur, mileage, (cast(date_format(date_created,'yyyy')  AS INT) - manufacture_year) AS yrs_driven
FROM clean_cars
ORDER BY mileage ASC, price_eur ASC, yrs_driven ASC
LIMIT 25;



SELECT
maker, model, price_eur


hive -e 'select * cars_db.clean_cars' | sed 's/[\t]/,/g'  > /clean_cars.csv
