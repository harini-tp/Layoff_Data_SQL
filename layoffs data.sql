# data cleaning project

SELECT * FROM layoffs;         #displaying the raw data

# 1-duplicates removed, 2-data standardization, 3-null or blanck values,  4-removing cloumns

CREATE TABLE layoffs_staging 
LIKE layoffs;

INSERT INTO layoffs_staging 
SELECT * FROM layoffs;             #creating a copy called staging to work with

SELECT * FROM layoffs_staging;        #displaying the new table

# 1- removing duplicates

SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, industry, total_laid_off, 'date') AS row_num   #this numbers with partition by column and if repeated - row num will be > than 1
FROM layoffs_staging;

WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER
(PARTITION BY company,location, industry, total_laid_off,percentage_laid_off, `date`, stage, country, funds_raised_millions) 
AS row_num                                                               
FROM layoffs_staging                          #creating a commom table expression to find the repeats
)
SELECT * FROM duplicate_cte WHERE row_num > 1;          #now rows with 2 are duplicates and can be deleted

#we couldnt just type delete instead of select on line above to delete the duplicates so create another table
#that includes the row num too

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,                              #right click layoffs_staging  -> copy to clipboard -> create statement -> paste
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT                                                         #included here
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT * FROM layoffs_staging2;     #another table - now insert values from the old one with the row_num

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER
(PARTITION BY company,location, industry, total_laid_off,percentage_laid_off, `date`, stage, country, funds_raised_millions) 
AS row_num                                                               
FROM layoffs_staging;

SELECT * FROM layoffs_staging2 WHERE row_num > 1;      #checking before deleting

DELETE FROM layoffs_staging2 WHERE row_num > 1;     #deleting

SELECT * FROM layoffs_staging2;          #we dont need row_num anymore though can remove it


# data standardization  -->  finding issues in data and fixing it

SELECT company, TRIM(company) FROM layoffs_staging2;   #there are some whote spaces -- will be cleared

UPDATE layoffs_staging2 SET company = TRIM(company);

SELECT DISTINCT industry FROM layoffs_staging2         #correcting industry row now
ORDER BY 1;

SELECT * FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';            #same catagory but some has crypto, crypto currency,cryptocurrency showed up seperately in distinct

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

SELECT DISTINCT industry FROM layoffs_staging2;    #all unique --  still some null values needs to be corrected

SELECT DISTINCT location FROM layoffs_staging2          #all good
ORDER BY 1;

SELECT DISTINCT country FROM layoffs_staging2          #US ans US with a dot are catagorised as different country..
ORDER BY 1;

SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2                                        #small trick with trim to remove period
ORDER BY 1;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

SELECT `date` FROM layoffs_staging2;         #in this database the date are of text type, this will be a problem in time scaling

SELECT `date`,                                 #this literaly changes string to date format where (date is passed in , and the format of 
STR_TO_DATE(`date`, '%m/%d/%Y') AS newone        #date in the column)       [ date format in sql : y/m/d ]  
FROM layoffs_staging2;         

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');        

#even though we changed str to date format the databace still remains as 'text' column not a 'date'-- to change it

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

# dealin with null and blank

SELECT * FROM layoffs_staging2;

SELECT * FROM layoffs_staging2
WHERE total_laid_off IS NULL           #the data is about laidoff and these two columns are important but few are empty
AND percentage_laid_off IS NULL;

#there where few null in industry but it could be filled up by refering to same companny names

SELECT company,industry FROM layoffs_staging2
WHERE industry = '' OR industry IS NULL
ORDER BY 1;

SELECT * FROM layoffs_staging2
WHERE company = 'Airbnb';

#airbnb one has industry and otherr doesnt so it can be usd=ed to fill out
#join the table with itself and see if a company with no industry has a industry on some otherr column for the same company

SELECT ls1.industry, ls2.industry
FROM layoffs_staging2 ls1
JOIN layoffs_staging2 ls2
ON ls1.company = ls2.company
WHERE (ls1.industry IS NULL OR ls1.industry = '')
AND (ls2.industry IS NOT NULL AND ls2.industry != '');

#and updating it

UPDATE layoffs_staging2 ls1
JOIN layoffs_staging2 ls2
ON ls1.company = ls2.company
SET ls1.industry = ls2.industry
WHERE (ls1.industry IS NULL OR ls1.industry = '')
AND (ls2.industry IS NOT NULL AND ls2.industry != '');

#all populated except 'bally' cause it didnt have a reference
#data like laidoff and percentage coul be filled if we had the total employee before and after but for now we cant do anything

#about the column with no laidoff and percent we can delete it

SELECT * FROM layoffs_staging2
WHERE total_laid_off IS NULL           
AND percentage_laid_off IS NULL;

DELETE FROM layoffs_staging2
WHERE total_laid_off IS NULL          
AND percentage_laid_off IS NULL;

#to remove rownum

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

SELECT * FROM layoffs_staging2;




#------Exploratory DA---------

SELECT * FROM layoffs_staging2;

SELECT MAX(total_laid_off), MAX(percentage_laid_off)
FROM layoffs_staging2;

SELECT * FROM layoffs_staging2
WHERE percentage_laid_off = 1;

SELECT * FROM layoffs_staging2
ORDER BY funds_raised_millions DESC;

SELECT company, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;

SELECT MAX(`date`), MIN(`date`) FROM layoffs_staging2; 

SELECT industry, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY industry
ORDER BY 2 DESC;

SELECT country, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY country
ORDER BY 2 DESC;

SELECT YEAR(`date`), SUM(total_laid_off)        #year function displays only year
FROM layoffs_staging2
GROUP BY YEAR(`date`)
ORDER BY 1 DESC;

SELECT SUBSTRING(`date`,6,2) AS `month`, SUM(total_laid_off) AS tot        #rolling value-- sum of layoff for each month
FROM layoffs_staging2                                                      #problem is each month include all 3 years
GROUP BY `month`;

SELECT SUBSTRING(`date`,1,7) AS `month`, SUM(total_laid_off) AS tot        
FROM layoffs_staging2  
WHERE SUBSTRING(`date`,1,7) IS NOT NULL                                                    #so include year too
GROUP BY `month`
ORDER BY 1 ASC;


WITH rolling AS
(
SELECT SUBSTRING(`date`,1,7) AS `month`, SUM(total_laid_off) AS tot        
FROM layoffs_staging2  
WHERE SUBSTRING(`date`,1,7) IS NOT NULL                                                    #ROLLING VALUE ON layoff
GROUP BY `month`
ORDER BY 1 ASC
)
SELECT `month`, tot, SUM(tot) OVER(ORDER BY `month`) AS rolled_up
FROM rolling;

SELECT company, SUM(total_laid_off) AS layoff_sum
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;

#to rank based on year for the company that had most layoff

SELECT company, YEAR(`date`), SUM(total_laid_off) AS layoff_sum
FROM layoffs_staging2
GROUP BY company,YEAR(`date`)
ORDER BY 3 DESC;

WITH company_yr(company, `year`, layoff_sum) AS
(
SELECT company, YEAR(`date`), SUM(total_laid_off) AS layoff_sum
FROM layoffs_staging2
GROUP BY company,YEAR(`date`)
ORDER BY 3 DESC
), Comp_rank AS
(
SELECT *, DENSE_RANK() OVER(PARTITION BY `year` ORDER BY layoff_sum DESC) AS ranking
FROM company_yr
WHERE `year` IS NOT NULL
)
SELECT * FROM Comp_rank
WHERE ranking <= 5;