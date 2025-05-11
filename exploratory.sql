select * from users;
show databases;
CREATE TABLE roles (
    role_code INT PRIMARY KEY,
    role_name VARCHAR(50)
);
INSERT INTO roles (role_code, role_name) VALUES
(1, 'Admin'),
(2, 'Editor'),
(3, 'Viewer');



create Table metal(
   id Int,name varchar(60),role_code INT,FOREIGN KEY (role_code) REFERENCES roles(role_code))
   ;
INSERT INTO metal (id, name, role_code) VALUES
(1, 'Alice', 1),
(2, 'Bob', 2),
(3, 'Charlie', 3),
(4, 'David', 1);

show tables;

select m.id,
m.name,
r.role_name as role
from metal m
join roles r on m.role_code = r.role_code;

-- data cleaning for the layoffs

SELECT * FROM layoffs;

-- steps:
-- 1. remove the duplicate
-- 2. standardize the data
-- 3. null values or blank values
-- 4. remove any columns or rows

-- creating the stagging as if we reomve the column in a rwa data then it may lead to problem so the staging method is created and for the purpose of the ETL

create table layoffs_stagging
like layoffs;

SELECT * FROM layoffs_stagging;



insert layoffs_stagging
select *
from layoffs;

-- remove the duplicates from the layoffs_stagging

SELECT *,
       ROW_NUMBER() OVER (
           PARTITION BY company,location, industry, total_laid_off, percentage_laid_off,"date",stage,country,funds_raised_millions
          ) AS row_num
FROM layoffs_stagging;

with duplicate_cte as(
SELECT *,
       ROW_NUMBER() OVER (
           PARTITION BY company, industry, total_laid_off, percentage_laid_off, "date"
          ) AS row_num
FROM layoffs_stagging
)
select *
from duplicate_cte
where row_num > 1;

CREATE TABLE `layoffs_stagging3` (
  `company` TEXT,
  `location` TEXT,
  `industry` TEXT,
  `total_laid_off` INT DEFAULT NULL,
  `percentage_laid_off` TEXT,
  `date` TEXT,
  `stage` TEXT,
  `country` TEXT,
  `funds_raised_millions` INT DEFAULT NULL ,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select *
from layoffs_stagging3
where row_num>1;

INSERT INTO layoffs_stagging3
SELECT *,
       ROW_NUMBER() OVER (
           PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
       ) AS row_num
FROM layoffs_stagging;

SET SQL_SAFE_UPDATES = 0;


DELETE
from layoffs_stagging3
where row_num>1;


select *
from layoffs_stagging3;

-- standarizing
select company,trim(company)
from layoffs_stagging3;

update layoffs_stagging3
set company=trim(company);

select distinct industry
from layoffs_stagging3
order by 1;

select *
from layoffs_stagging3
where industry like 'crypto';

update layoffs_Stagging3
set industry='crypto'
where industry like 'crypto%';

select distinct location
from layoffs_stagging3
order by 1;

select distinct country,trim(trailing '.' from country)
from layoffs_stagging3
order by 1;

SELECT `date`
FROM layoffs_stagging3;



update layoffs_stagging3
set country=trim(trailing '.' from country)
where country like 'united states%';

UPDATE layoffs_stagging3
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y')
WHERE `date` LIKE '%/%/%';

ALTER TABLE layoffs_stagging3
MODIFY COLUMN `date` DATE;

select *
from layoffs_stagging3;


SELECT *
FROM layoffs_stagging3
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

select *
from layoffs_stagging3
where industry is null
or industry='';

select *
from layoffs_stagging3
where company='Airbnb';

update layoffs_stagging3
set industry=null
where industry='';

select *
from layoffs_stagging3 st1
join layoffs_stagging3 st2
   on st1.company=st2.company
where (st1.industry is null or st1.industry='')
and st2.industry is not null;

UPDATE layoffs_stagging3 st1
JOIN layoffs_stagging3 st2
  ON st1.company = st2.company
  AND st1.location = st2.location
SET st1.industry = st2.industry
WHERE st1.industry IS NULL 
  AND st2.industry IS NOT NULL;
  


delete
FROM layoffs_stagging3
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT *
FROM layoffs_stagging3;

alter table layoffs_stagging3
drop column row_num;



-- Exploratory data analysis

select *
from layoffs_stagging3;

select MAX(total_laid_off) ,max(percentage_laid_off)
from layoffs_stagging3;

select *
from layoffs_stagging
where percentage_laid_off=1
order by funds_raised_millions desc;

select company ,sum(total_laid_off)
from layoffs_stagging
group by company
order by 2 desc;

select year('date') ,sum(total_laid_off)
from layoffs_stagging
group by year('date')
order by 2 desc;

select company,avg(percentage_laid_off)
from layoffs_stagging3
group by company
order by 2 desc;

select substring(`date`,6,2) as month ,sum(total_laid_off)
from layoffs_stagging3
where substring(`date`,6,2) is not null
group by month
order by 1 asc
;


with rolling_table as(
select substring(`date`,6,2) as month ,sum(total_laid_off) as total_off
from layoffs_stagging3
where substring(`date`,6,2) is not null
group by month
order by 1 asc
)
select 'month',total_off,
sum(total_off) over(order by 'month')as rolling_total
from rolling_table;

with company_year(company,years,total_laid_off )as(
select company,year(`date`),sum(total_laid_off)
from layoffs_stagging3
group by company,year(`date`)	
),company_year_rank as(
select *,dense_rank() over(partition by years order by total_laid_off desc) as ranking
from company_year
where years is not null)
select *
from company_year_rank
where ranking <=5; 	





