-- Data Cleaning
select count(*) from  layoffs;
select * from layoffs;
create table layoffs_staging like layoffs;
select * from layoffs_staging;
insert layoffs_staging select * from layoffs;


with duplicate_cte as (
select *, row_number() over(partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num from layoffs_staging) select * from duplicate_cte where row_num >1;

select * from layoffs_staging where company  = 'Oda';
select count(*) from layoffs_staging;

select * from layoffs_staging order by  company, location, industry;

select * from layoffs_staging where company = 'Casper';


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

delete from layoffs_staging2 where row_num >1;
select count(*) from layoffs_staging2 ;


insert into layoffs_staging2
select *, row_number() over(partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num from layoffs_staging;


-- Standardizing Data
 
select company , trim(company) from layoffs_staging;
select sum(length(company )), sum(length(trim(company))) from layoffs_staging;

update layoffs_staging2 set company = trim(company);
select distinct industry, industry from layoffs_staging2 order by 1; 
select * from layoffs_staging2 ;
select distinct industry  from layoffs_staging2 order by industry ;
select * from layoffs_staging2 where industry like 'Crypto%';
update layoffs_staging2 
set industry = 'Crypto'
where industry like 'Crypto%';
select distinct country from layoffs_staging2  where country like 'United States%' order by 1;

update layoffs_staging2 
set country = 'United States' where country like 'United States%' ;

select date from layoffs_staging2;
select `date` from layoffs_staging2 order by 1;

update layoffs_staging2 
set `date` = str_to_date(`date`, '%m/%d/%Y');

alter table layoffs_staging2 
modify column `date` date;
select * from layoffs_staging2 where company = 'Airbnb';
select * from layoffs_staging2 where  total_laid_off is null and percentage_laid_off is null ;
SELECT 
    t1.industry, t2.industry
FROM
    layoffs_staging2 AS t1
        JOIN
    layoffs_staging2 AS t2 ON t1.company = t2.company
WHERE
    (t1.industry IS  NULL
        OR t1.industry = '')
        AND t2.industry IS NOT NULL;

 update layoffs_staging2 set industry  = null where industry = '';
 
 update layoffs_staging2 t1  join layoffs_staging2 t2  on t1.company = t2.company set t1.industry = t2.industry  where t1.industry is null and t2.industry is  not null;
 select * from layoffs_staging2 where industry is null or industry  = '';
 select * from layoffs_staging2;
 delete from layoffs_staging2  where percentage_laid_off is null and total_laid_off is null;
 alter table layoffs_staging2
 drop column row_num;
 
 
 
 
-- Exploaratory Data Analysis
  
select max(total_laid_off) as max_tot_laid_offs , concat((max(percentage_laid_off)*100), '%') as max_perc_laid_off from layoffs_staging2;
select * from layoffs_staging2 where percentage_laid_off =1;
select industry, sum(total_laid_off) from layoffs_staging2 where location = 'mumbai' group by industry order by 2 desc;

select min(`date`), max(`date`) from layoffs_staging2;

select year(`date`), sum(total_laid_off) from layoffs_staging2  group by year(`date`) order by 2,1 desc;

with rolling_total as (
select substring(`date`, 1,7) as `month`, sum(total_laid_off) as total_off from layoffs_staging2  group by `month` having `month` not like 'null' order by 2 desc
)
select `month` ,total_off, sum(total_off) over(order by `month`) as rolling_total_ from rolling_total;

with Company_Years (company, years, total_laid_off) as 
(
select company, year(`date`), sum(total_laid_off) from layoffs_staging2  group by company, year(`date`) order by 3 desc,1 desc
), Company_rank_year as 
(select *, dense_rank() over (partition by years order by total_laid_off)  as ranking from Company_Years where years is not null ) 
select * from Company_rank_year where ranking <=5;

