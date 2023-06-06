-- Using tables and views from 'table_and_query_creation.sql' to write queries

-- Finding salary ranges and avg salary for each unique job title
SELECT 
    job_title,
    ROUND(AVG(salary_in_usd),2) AS avg_salary,
    MAX(salary_in_usd) AS max_salary,
    MIN(salary_in_usd) AS min_salary
FROM ds_salaries
GROUP BY job_title
ORDER BY min_salary DESC;

-- Finding job titles and salaries of countries with average salaries <= USD $50000
SELECT
    job_title, 
    ROUND(AVG(salary_in_usd),2) AS avg_salary, 
    employee_residence_country
FROM v_ds_salaries_nice 
WHERE employee_residence_country IN (
    SELECT employee_residence_country 
    FROM v_ds_salaries_nice
    GROUP BY employee_residence_country
    HAVING MAX(salary_in_usd) <= 50000
)
GROUP BY job_title, employee_residence_country
HAVING ROUND(AVG(salary_in_usd),2) <= 50000
ORDER BY avg_salary ASC;

-- Finding job titles with salaries >= USD $100000 and either freelancer or contractor emplyment types
(SELECT 
    job_title, salary_in_usd, employment_type
 FROM v_ds_salaries_nice 
 WHERE salary_in_usd >= 100000 AND employment_type = 'Freelancer'
)
UNION
(SELECT 
    job_title, salary_in_usd, employment_type
 FROM v_ds_salaries_nice 
 WHERE salary_in_usd >= 100000 AND employment_type = 'Contractor'
)
ORDER BY salary_in_usd DESC;

-- Finding common job titles at small and large companies with their salaries and experience levels
(SELECT 
    job_title, salary_in_usd, experience_level
 FROM v_ds_salaries_nice 
 WHERE company_size = 'Small'
)
INTERSECT
(SELECT 
    job_title, salary_in_usd, experience_level
 FROM v_ds_salaries_nice 
 WHERE company_size = 'Large'
)
ORDER BY salary_in_usd DESC;

-- Finding offshore employees whose salary is between USD $50000 and $100000
SELECT *
FROM v_us_offshore_employees
WHERE salary_in_usd BETWEEN 50000 AND 100000;

-- Finding number of offshore employees and their countries
-- [Scalar Subquery]
SELECT DISTINCT
    employee_residence_country,
        (SELECT COUNT(*)
         FROM v_us_offshore_employees AS v2
         WHERE v1.employee_residence_country = v2.employee_residence_country
        )
    AS num_employees
FROM v_us_offshore_employees AS v1
ORDER BY num_employees DESC;

-- Finding job characteristics with greater than dataset average salary
WITH avg_salary (value) AS
    (SELECT AVG(salary_in_usd) 
     FROM v_ds_salaries_nice)
SELECT 
    v.job_title, v.salary_in_usd, 
    v.employee_residence, v.experience_level, 
    v.employment_type, v.remote_ratio, 
    v.company_size
FROM v_ds_salaries_nice AS v, avg_salary
WHERE v.salary_in_usd >= avg_salary.value
ORDER BY salary_in_usd DESC;

-- Defining a procedure to obtain monthly salary
CREATE PROCEDURE monthly_salary()
LANGUAGE plpgsql AS $$
    DECLARE
    total float;
    BEGIN
        SELECT ROUND(salary_in_usd / 12, 2)
        FROM v_us_offshore_employees;
        --RETURN total;
    END;
$$ 

CALL monthly_salary();

-- Defining a function to obtain monthly salary
CREATE FUNCTION func_monthly_salary()
RETURNS float
LANGUAGE plpgsql AS $$
    DECLARE
        monthly_salary_in_usd float;
    BEGIN
        SELECT ROUND(salary_in_usd / 12, 2)
            AS monthly_salary_in_usd
        FROM v_us_offshore_employees;
        RETURN monthly_salary_in_usd;
    END;
$$;

SELECT func_monthly_salary();