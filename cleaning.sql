# Cleaning Project
select *
from layoffs;


-- 1. Remove Duplicates
-- 2. Standardize the Data
-- 3. Null Values or blank values
-- 4. Remove any Columns

-- Make a copy of the table prior to updating it
CREATE TABLE layoffs_staging
LIKE layoffs;

-- Insert data from the original table into the copy
SELECT *
FROM layoffs_staging

INSERT layoffs_staging
SELECT *
FROM layoffs;

SELECT *
FROM layoffs_staging

-- 1. Remove Duplicates
--ROW_NUMBER() assigns a unique sequential number to each row, ignoring ties. 12345

SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`) AS row_num
FROM layoffs_staging;

-- A ROW_NUMBER() value >= 2 indicates duplicate rows within the partition
-- Identify duplicate rows in layoffs_staging based on company, industry, total_laid_off, percentage_laid_off, and date using a CTE and ROW_NUMBER()
WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num >1;

-- Select rows with row_num > 1 to confirm duplicates
SELECT *
FROM layoffs_staging
where company = 'Oda'; 

-- No duplicates found; need to partition by all columns to accurately detect duplicates

WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num >1;

-- Select rows with row_num > 1 to confirm duplicates
SELECT *
FROM layoffs_staging
where company = 'Yahoo'; 

-- Remove duplicate rows from layoffs_staging by keeping only the first occurrence per group using a CTE and ROW_NUMBER()
WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
DELETE 
FROM duplicate_cte
WHERE row_num >1;
-- The target  table duplicate_cte of teh DELELTE is not updatable

SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging


-- Create script for layoffs_staging2 from layoff_staging and add row_num column
CREATE TABLE `layoffs_staging2`(
`company` text,
`location` text, 
`industry` text,
`total_laid_off` int DEFAULT NULL,
`percentage_laid_off` text,
`date` text,
`stage` text,
`country` text,
`funds_raised_millions` int DEFAULT NULL,
`row_num` INT
)ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;



-- Insert data from layoffs_staging into layoffs_staging2
INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

-- Select row_num >1 to find duplicates 
SELECT *
FROM layoffs_staging2
WHERE row_num >1 ;

-- Delete duplicates
DELETE 
FROM layoffs_staging2
WHERE row_num >1 ;

SELECT *
FROM layoffs_staging2
WHERE row_num >1 ;

SELECT *
FROM layoffs_staging2

--2. Standardizing data

-- Select distinct company names from layoffs_staging2
SELECT DISTINCT(company)
from layoffs_staging2

--Trim the company names
SELECT company, TRIM(company)
from layoffs_staging2

 --Update company to TRIM(company) in layoffs_staging2
UPDATE layoffs_staging2
SET company =TRIM(company);

SELECT *
FROM layoffs_staging2;

-- Select distinct industry names from layoffs_staging2 order by industry names
SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1;

-- Select all rows from layoffs_staging2 where industry starts with 'Crypto'
SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

-- Standardize industry names beginning with 'Crypto' to 'Crypto'
UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

SELECT distinct industry
FROM layoffs_staging2;

-- Select distinct location names from layoffs_staging2 order by location names
SELECT distinct location
FROM layoffs_staging2
ORDER BY 1;

-- Select distinct country names from layoffs_staging2 order by country names
SELECT distinct country
FROM layoffs_staging2
ORDER BY 1;

SELECT *
FROM layoffs_staging2
WHERE country LIKE 'United States%'
ORDER BY 1;

-- Get unique country names with trailing periods removed
SELECT distinct country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY 1;

-- Standardize industry names beginning with 'United States' to 'United States'
UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

SELECT distinct country
FROM layoffs_staging2
ORDER BY 1;

SELECT *
FROM layoffs_staging2

-- Change data type from text to datetime for proper time handling
SELECT `date`,
STR_TO_DATE(`date`,'%m/%d/%Y')
FROM layoffs_staging2;

-- Update column `date` from text to datetime using STR_TO_DATE
UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`,'%m/%d/%Y')

SELECT *
FROM layoffs_staging2;

-- Change `date` column type from text to DATE
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE; 


-- 3. Null Values or blank values
SELECT *
FROM layoffs_staging2;

-- Select rows where both total_laid_off and percentage_laid_off are NULL
SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Select rows where industry is NULL or blank
SELECT DISTINCT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = '';

SELECT *
FROM layoffs_staging2
WHERE company = 'Airbnb';

-- Find rows where a company's industry is missing in t1 but exists in another row t2
SELECT *
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
WHERE (t1.industry IS NULL or t1.industry = '')
AND t2.industry IS NOT NULL

-- Show pairs of industry values where a company's industry is missing in t1 but exists in another row t2
SELECT t1.industry, t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
WHERE (t1.industry IS NULL or t1.industry = '')
AND t2.industry IS NOT NULL;

-- Convert blank industry values to NULL
UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

-- Update NULL industry values by copying non-NULL industry values from the same company
UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL 
AND t2.industry IS NOT NULL;

SELECT *
FROM layoffs_staging2
WHERE company = 'Airbnb';

-- Review rows where industry is NULL or blank
SELECT DISTINCT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = '';

-- Find all companies with names starting with 'Bally'
SELECT *
FROM layoffs_staging2
WHERE company LIKE  'Bally%';

SELECT *
FROM layoffs_staging2

-- Review rows where total_laid_off and percentage_laid_off are both NULL
SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Delete rows where total_laid_off and percentage_laid_off are both NULL
DELETE 
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- 4. Remove any Columns
SELECT *
FROM layoffs_staging2

-- Drop the row_num column from layoffs_staging2
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

SELECT *
FROM layoffs_staging2


