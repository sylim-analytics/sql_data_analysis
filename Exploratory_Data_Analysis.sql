--Exploratory Data Analysis

select *
from layoffs_staging2;

select max(total_laid_off) , max(percentage_laid_off)
from layoffs_staging2;

select *
from layoffs_staging2
where percentage_laid_off = 1
order by total_laid_off DESC;

select *
from layoffs_staging2
where percentage_laid_off = 1
order by funds_raised_millions DESC;

select company, sum(total_laid_off)
from layoffs_staging2
group by company
order by 2 desc;

select min(`date`), max(`date`)
from layoffs_staging2;

select company, sum(total_laid_off) 
from layoffs_staging2
group by company
order by 2 desc;

select industry, sum(total_laid_off) 
from layoffs_staging2
group by industry
order by 2 desc;

select country, sum(total_laid_off) 
from layoffs_staging2
group by country
order by 2 desc;

select year(`date`), sum(total_laid_off) 
from layoffs_staging2
group by year(`date`)
order by 1 desc;

select stage, sum(total_laid_off) 
from layoffs_staging2
group by stage
order by 2 desc;

-- Monthly total_laid_off using SUBSTRING(date, 1, 7), sorted by month (YYYY-MM)
select substring(`date`,1,7) as `month`, sum(total_laid_off)
from layoffs_staging2
where substring(`date`,1,7) is not null
group by `month`
order by 1 ASC; 

-- Monthly total_laid_off with cumulative (rolling) total
with Rolling_Total as
(
select substring(`date`,1,7) as `month`, sum(total_laid_off) as total_off
from layoffs_staging2
where substring(`date`,1,7) is not null
group by `month`
order by 1 ASC
)
select `month`, total_off,
sum(total_off) over(order by `month`) as rolling_total
from Rolling_Total;

-- Yearly total_laid_off by company
select company, year(`date`), sum(total_laid_off) 
from layoffs_staging2
group by company, year(`date`)
order by company ASC;

-- Yearly total_laid_off by company, sorted by total layoffs (descending)
select company, year(`date`), sum(total_laid_off) 
from layoffs_staging2
group by company, year(`date`)
order by 3 desc;

-- Yearly ranking of companies by total_laid_off (highest to lowest)
with Company_Year (company, years, total_laid_off)  as
(
select company, year(`date`), sum(total_laid_off) 
from layoffs_staging2
group by company, year(`date`)
)
select *, dense_rank() over (partition by years order by total_laid_off desc) as Ranking
from Company_Year
where years is not null
order by Ranking asc;

-- Top 5 companies per year ranked by total_laid_off
with Company_Year (company, years, total_laid_off)  as
(
select company, year(`date`), sum(total_laid_off) 
from layoffs_staging2
group by company, year(`date`)
), Company_Year_Rank as
(select *, dense_rank() over (partition by years order by total_laid_off desc) as Ranking
from Company_Year
where years is not null
)
select *
from Company_Year_Rank
where Ranking <=5
;
