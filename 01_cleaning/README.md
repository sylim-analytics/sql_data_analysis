# Cleaning Project


select *
from layoffs;

--1. Remove Duplicates
--2. Standardize the Data
--3. Null Values or blank values
--4. Remove Any Columns

create table layoffs_staging
like layoffs;

select *
from layoffs_staging;

insert layoffs_staging
select *
from layoffs;

select *,
ROW_NUMBER() OVER(
partition by company, industry, total_laid_off, percentage_laid_off, 'date') as row_num
from layoffs_staging;


With duplicate_cte as
(
select *,
ROW_NUMBER() OVER(
partition by company, location, industry, total_laid_off, percentage_laid_off, date, stage, country, funds_raised_millions) as row_num
from layoffs_staging
)
select *
from duplicate_cte
where row_num > 1;

select *
from layoffs_staging
where company = 'Better.com';

select *
from layoffs_staging
where company = 'Casper';

With duplicate_cte as
(
select *,
ROW_NUMBER() OVER(
partition by company, location, industry, total_laid_off, percentage_laid_off, date, stage, country, funds_raised_millions) as row_num
from layoffs_staging
)
delete 
from duplicate_cte
where row_num > 1;


CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;



select *
from layoffs_staging2;

insert into layoffs_staging2
select *,
ROW_NUMBER() OVER(
partition by company, location, industry, total_laid_off, percentage_laid_off, date, stage, country, funds_raised_millions) as row_num
from layoffs_staging;

select *
from layoffs_staging2
where row_num >1;


SET SQL_SAFE_UPDATES = 0;

DELETE 
FROM layoffs_staging2
WHERE row_num > 1;

select *
FROM layoffs_staging2
WHERE row_num > 1;

select *
FROM layoffs_staging2

--standarizing data

SELECT company, trim(company)
FROM layoffs_staging2;

update layoffs_staging2
set company = trim(company);


SELECT distinct industry
FROM layoffs_staging2
order by 1;

select *
from layoffs_staging2
where industry like 'Crypto%';

update layoffs_staging2
set industry = 'Crypto'
where industry like 'Crypto%';

SELECT distinct industry
FROM layoffs_staging2
;

select distinct country, trim(trailing '.' from country)
from layoffs_staging2
order by 1;

update layoffs_staging2
set country = trim(trailing '.' from country)
where country like 'United States%%';

SELECT `date`,
str_to_date(`date`,'%m/%d/%Y')
FROM layoffs_staging2;

update layoffs_staging2
set `date` = str_to_date(`date`,'%m/%d/%Y');

SELECT `date`
FROM layoffs_staging2;

alter table layoffs_staging2
modify column `date` date;

select * 
FROM layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

select *
FROM layoffs_staging2
where industry is null
or industry = '';

update layoffs_staging2
set industry = null
where industry = '';

select *
from layoffs_staging2
where company like 'Bally%';

select t1.industry, t2.industry
from layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company = t2.company
    AND t1.location = t2.location
where (t1.industry is null or t1.industry = '')
and t2.industry is not null;

update layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company = t2.company
set t1.industry = t2.industry
where (t1.industry is null )
and t2.industry is not null;

select *
from layoffs_staging2;

select *
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

delete
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

select *
from layoffs_staging2;

alter table layoffs_staging2
drop column row_num;
