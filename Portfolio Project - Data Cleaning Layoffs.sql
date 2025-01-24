-- SQL Project - Data Cleaning

-- https://www.kaggle.com/datasets/swaptr/layoffs-2022


-- View the data
SELECT * 
FROM world_layoffs.layoffs;


-- First thing is to create a staging table. This is the one we will work in and clean the data. We want a table with the raw data in case something happens
CREATE TABLE world_layoffs.layoffs_staging 
LIKE world_layoffs.layoffs;

INSERT layoffs_staging 
SELECT * FROM world_layoffs.layoffs;

-- Steps to be followed in the data cleaning process
-- 1. check for duplicates and remove any
-- 2. standardize data and fix errors
-- 3. Look at null values and see what 
-- 4. remove any columns and rows that are not necessary - few ways

-- 1. REMOVING DUPLICATES
# First let's check for duplicates
-- The query below assigns each individual a number, if there is a duplicate it'll have a number greater than 1

SELECT company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions,
ROW_NUMBER() OVER (
	PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions) AS row_num
FROM world_layoffs.layoffs_staging;

-- View only the duplicates
SELECT *
FROM (
	SELECT company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions
			) AS row_num
	FROM  world_layoffs.layoffs_staging ) duplicates
WHERE 	row_num > 1;
-- these are the ones we want to delete where the row number is > 1 or 2or greater essentially

-- To delete only the rows with >1 i created a new column and add those row numbers in. Then delete where row numbers are over 2, then delete that column
-- Create a new column
ALTER TABLE world_layoffs.layoffs_staging ADD row_num INT;
-- Check if new column created
SELECT *
FROM world_layoffs.layoffs_staging
;

-- Create a new table layoff_staging2
CREATE TABLE world_layoffs.layoffs_staging2
like world_layoffs.layoffs_staging;

-- Insert into the second table all values in the first staging table but the last column we define it.
INSERT INTO `world_layoffs`.`layoffs_staging2`
(`company`,`location`,`industry`,`total_laid_off`,`percentage_laid_off`,`date`,
`stage`,`country`,`funds_raised_millions`,`row_num`)
SELECT `company`,`location`, `industry`,`total_laid_off`,`percentage_laid_off`,
`date`,`stage`,`country`,`funds_raised_millions`, ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions
			) AS row_num
	FROM world_layoffs.layoffs_staging;
    
-- Check new table, layoffs_staging2
select *
from layoffs_staging2;

-- Delete all rows the row_num is greater than 2 or 2
DELETE FROM world_layoffs.layoffs_staging2
WHERE row_num >= 2;

-- NO DUPLICATES

---------------------------------------------------------------------------------------------------------
-- 2. DATA STANDARDIZATION
-- Standardizing each column is the best approach here

SELECT * 
FROM world_layoffs.layoffs_staging2;

-- the industry has both null and empty rows, for analysis purposes we let all be null
UPDATE world_layoffs.layoffs_staging2
SET industry = NULL
WHERE industry = '';

-- To fill some missing values in the industry where same company has in one column industry label and the other no label.
-- An example;
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE company LIKE 'airbnb%';
-- we use join to join on self and update
-- now we need to populate those nulls if possible

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;
- ---------------------------------------------------

-- I also noticed the Crypto has multiple different variations. We need to standardize that - let's say all to Crypto
SELECT DISTINCT industry
FROM world_layoffs.layoffs_staging2
ORDER BY industry;

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry IN ('Crypto Currency', 'CryptoCurrency');

-- check, industry being standerdized.
SELECT DISTINCT industry
FROM world_layoffs.layoffs_staging2
ORDER BY industry;

-- ------------------------------------------------------------------------
-- we also need to look at country 
SELECT country
FROM world_layoffs.layoffs_staging2;

-- We have some "United States" and some "United States." with a period at the end. Let's standardize this.
-- by using trailing we select the last unsupported character .
UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country);

-- Fixed Check
SELECT DISTINCT country
FROM world_layoffs.layoffs_staging2
ORDER BY country;
---------------------------------------------------------------------------

-- SETTING THE DATE
-- Initially when uploading didn't set our column date to date. 
SELECT *
FROM world_layoffs.layoffs_staging2;

-- Using Str_to_date command we convert it to date.
-- Note we are using backticks because date is a function in sql
UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

-- convert the data type of the column 'date` from text to date
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;
-- check
SELECT *
FROM world_layoffs.layoffs_staging2;


-- 3. NULL VALUES
-- There isn't much we can do about the nulls in total layoff, percentage layoffs and raised amounts, since
-- there is no much data to make computations etc so we leave them for EDA anlysis.

-- 4. REMOVING COLUMNS NOT NEEDED
SELECT *
FROM world_layoffs.layoffs_staging2;

-- We remove rows that total laid off and percentage laid off is null (useless data)
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Delete Useless data we can't really use
DELETE FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Check the nulls have reduced
SELECT * 
FROM world_layoffs.layoffs_staging2;

-- Drop the column row_num not useful
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

-- Final cleaned data
SELECT * 
FROM world_layoffs.layoffs_staging2;


































