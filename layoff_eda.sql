CREATE DATABASE company_db;
USE company_db;

SELECT * FROM layoffs;


CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT * FROM layoffs_staging; #CREATES EMPTY TABLE

INSERT INTO layoffs_staging
SELECT * FROM layoffs;

SELECT * FROM layoffs_staging;

#--------------------------STEP 1: REMOVE DUPLICATES---------------------------------------------------

WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(partition by company,location,industry,total_laid_off,percentage_laid_off,stage,country,funds_raised,`date`) AS row_num 
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte 
WHERE row_num > 1;

#WE ARE CREATING ANOTHER TABLE WHERE THE DUPLICATE IS REMOVED

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `total_laid_off` text,
  `date` text,
  `percentage_laid_off` text,
  `industry` text,
  `source` text,
  `stage` text,
  `funds_raised` int DEFAULT NULL,
  `country` text,
  `date_added` text,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(partition by company,location,industry,total_laid_off,percentage_laid_off,stage,country,funds_raised,`date`) AS row_num 
FROM layoffs_staging;

SELECT * FROM layoffs_staging2
WHERE row_num >1;

DELETE 
FROM layoffs_staging2
WHERE row_num >1;

SELECT * FROM layoffs_staging2;

#-----------------STEP2: STANDARDIZING THE DATA(FINDING ISSUES IN THE DATA)-----------------------------------------------------
SELECT company,TRIM(company) from layoffs_staging2;

UPDATE layoffs_staging2
SET company=TRIM(company);

SELECT DISTINCT industry from layoffs_staging2
ORDER BY 1; 


SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1;


UPDATE layoffs_staging2
SET `date`= STR_TO_DATE(`date`,'%m/%d/%Y');

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

SELECT * FROM layoffs_staging2;

#---------------------STEP 3: NULL VALUES----------------------------------------

SELECT * FROM layoffs_staging2;

SELECT *
FROM layoffs_staging2 
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT * 
FROM layoffs_staging2
WHERE total_laid_off =0
AND percentage_laid_off =0;

SELECT * 
FROM layoffs_staging2
WHERE total_laid_off='';


SELECT *
FROM layoffs_staging2
WHERE industry IS NULL 
OR  industry='';

SELECT *
FROM layoffs_staging2
WHERE company='Appsmith';

SELECT company, industry
FROM layoffs_staging2
WHERE company = 'Appsmith'
  AND industry IS NOT NULL;


SELECT *
FROM layoffs_staging2
WHERE total_laid_off =''
AND percentage_laid_off='';

UPDATE layoffs_staging2
SET total_laid_off = NULL 
WHERE total_laid_off=''  ;

SELECT *
FROM layoffs_staging2
WHERE  percentage_laid_off='';

UPDATE layoffs_staging2
SET percentage_laid_off = NULL
WHERE percentage_laid_off='';

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL 
AND percentage_laid_off IS NULL;

DELETE 
FROM layoffs_staging2
WHERE total_laid_off IS NULL 
AND percentage_laid_off IS NULL;

SELECT *
FROM layoffs_staging2;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

SELECT * FROM layoffs_staging2;

#-------------------------EXPLORATORY DATA ANALYSIS----------------
SELECT * FROM layoffs_staging2;

SELECT MAX(total_laid_off),MAX(percentage_laid_off)
FROM layoffs_staging2;

#CHECKING WHICH COMPANIES HAD HIGHEST LAY OFF
SELECT * 
FROM layoffs_staging2
WHERE percentage_laid_off=1.0
ORDER BY total_laid_off DESC; 

#TOP COMPANIES OVERALL
SELECT company, SUM(total_laid_off) AS total
FROM layoffs_staging2
GROUP BY company
ORDER BY total DESC;

#CHECKING HIGHEST LAYOFF PER COMPANY
SELECT company,SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;

#CHECKING HIGHEST LAYOFF PER INDUSTRY
SELECT industry,SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY industry
ORDER BY 2 DESC;

#LAYOFFS BY INDUSRTY
SELECT industry, SUM(total_laid_off) AS total
FROM layoffs_staging2
GROUP BY industry
ORDER BY total DESC;

#CHECKING HIGHEST LAYOFF PER COUNTRY
SELECT country,SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY country
ORDER BY 2 DESC;


#CHECKING RANGE OF DATE
SELECT MIN(`date`),MAX(`date`) 
FROM layoffs_staging2;

#CHECKING PER YEAR LAY OFF
SELECT YEAR(`date`),SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY YEAR(`date`)
ORDER BY 1 DESC;

#CHECKING PER STAGES OF THE COMPANY
SELECT stage,SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY stage
ORDER BY 2 DESC;

##CHECKING PER MONTH
SELECT `date` FROM layoffs_staging2;
SELECT SUBSTRING(`date`,1,7) AS `MONTH`, SUM(total_laid_off) AS total_layoff #2 is how many characters will u take
FROM layoffs_staging2
GROUP BY `MONTH`
ORDER BY 1 ASC;

#ROLLING TOTAL CALCULATION
WITH Rolling_total AS
(
SELECT SUBSTRING(`date`,1,7) AS `MONTH`, SUM(total_laid_off) AS total_layoff #2 is how many characters will u take
FROM layoffs_staging2
GROUP BY `MONTH`
ORDER BY 1 ASC
)
SELECT `MONTH`,total_layoff, 
SUM(total_layoff) OVER(ORDER BY `MONTH`) AS rolling_total #It adds the layoffs using SUM(), and because of OVER(ORDER BY month), it keeps adding them month by month instead of restarting.
FROM Rolling_total;

SELECT company,YEAR(`date`),SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company,YEAR(`date`)
ORDER BY 3 DESC;

#OVERALL RANKING
WITH company_year(company,years,total_laid_off) AS  #() is used for changing column names
(
SELECT company,YEAR(`date`),SUM(total_laid_off) 
FROM layoffs_staging2
GROUP BY company,YEAR(`date`)
ORDER BY 3 DESC
),
company_year_rank AS (SELECT *,
DENSE_RANK() OVER(partition by years ORDER BY total_laid_off DESC) AS ranking #i am gonna dense_rank over()
FROM company_year)
SELECT * FROM company_year_rank
WHERE ranking<=5;









