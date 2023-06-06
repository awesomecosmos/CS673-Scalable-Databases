-- creating main table
CREATE TABLE ds_salaries (
    id INT PRIMARY KEY,
    work_year INT,
    experience_level VARCHAR(2),
    employment_type VARCHAR(2),
    job_title VARCHAR(255),
    salary INT,
    salary_currency VARCHAR(3),
    salary_in_usd INT,
    employee_residence VARCHAR(2),
    remote_ratio INT,
    company_location VARCHAR(2),
    company_size VARCHAR(2)
);

-- copying CSV data into table
COPY ds_salaries
FROM '/Users/av15397n/Documents/GitHub/CS673-Scalable-Databases/Project 1/data/ds_salaries_fixed.csv'
DELIMITER ','
CSV HEADER;

-- checking values copied over
SELECT * FROM ds_salaries;

-- creating country code table
CREATE TABLE iso3166 (
    row_num INT,
    name VARCHAR(255),
    alpha_2 VARCHAR(2) PRIMARY KEY,
    alpha_3 VARCHAR(3),
    country_code INT,
    iso_3166_2 VARCHAR(255),
    region VARCHAR(255),
    sub_region VARCHAR(255),
    intermediate_region VARCHAR(255),
    region_code INT,
    sub_region_code INT,
    intermediate_region_code INT
);

-- copying CSV data into table
COPY iso3166
FROM '/Users/av15397n/Documents/GitHub/CS673-Scalable-Databases/Project 1/data/iso_3166_fixed.csv'
DELIMITER ','
CSV HEADER;

-- checking values copied over
SELECT * FROM iso3166;

-- creating country currencies table
CREATE TABLE iso4217 (
    row_num INT PRIMARY KEY,
    Entity VARCHAR(255),
    Currency VARCHAR(255),
    AlphabeticCode VARCHAR(3),
    NumericCode INT,
    MinorUnit INT,
    WithdrawalDate VARCHAR(255)
);

-- copying CSV data into table
COPY iso4217
FROM '/Users/av15397n/Documents/GitHub/CS673-Scalable-Databases/Project 1/data/iso_4217_fixed.csv'
DELIMITER ','
CSV HEADER;

-- checking values copied over
SELECT * FROM iso4217;

-- creating secondary table
CREATE TABLE experience_levels (
    level_code VARCHAR(2) PRIMARY KEY,
    level_description VARCHAR(255)
);

-- inserting values
INSERT INTO experience_levels VALUES 
    ('EX','Executive'),
    ('MI','Mid/Intermediate'),
    ('EN','Entry-Level'),
    ('SE','Senior');

-- checking values
SELECT * FROM experience_levels;

-- creating secondary table
CREATE TABLE employment_types (
    type_code VARCHAR(2) PRIMARY KEY,
    type_description VARCHAR(255)
);

-- inserting values
INSERT INTO employment_types VALUES 
    ('PT','Part-Time'),
    ('FL','Freelancer'),
    ('FT','Full-Time'),
    ('CT','Contractor');

-- checking values
SELECT * FROM employment_types;

-- creating secondary table
CREATE TABLE company_size (
    size_code VARCHAR(2) PRIMARY KEY,
    size_description VARCHAR(255)
);

-- inserting values
INSERT INTO company_size VALUES 
    ('S','Small'),
    ('M','Medium'),
    ('L','Large');

-- checking values
SELECT * FROM company_size;

-- creating indexing for each table
CREATE INDEX ds_salaries_index ON ds_salaries(id);
CREATE INDEX iso3166_index ON iso3166(row_num);
CREATE INDEX iso4217_index ON iso4217(row_num);

-- adding FK constraint on ds_salaries referencing iso3166
ALTER TABLE ds_salaries
    ADD CONSTRAINT fk_employee_residence FOREIGN KEY (employee_residence) REFERENCES iso3166(alpha_2),
    ADD CONSTRAINT fk_company_location FOREIGN KEY (company_location) REFERENCES iso3166(alpha_2);

-- Joining ISO4217 info on ds_salaries data
CREATE VIEW v_ds_salaries_with_currency_info AS
    SELECT DISTINCT
        d.*,
        i.entity, i.currency, i.alphabeticcode
    FROM ds_salaries AS d
    LEFT JOIN iso4217 AS i 
        ON d.salary_currency = i.AlphabeticCode;

SELECT * FROM v_ds_salaries_with_currency_info;

-- Creating 'nice' dataset with readable value instead of obscure codes
CREATE VIEW v_ds_salaries_nice AS
    SELECT 
        d.id, d.work_year, 
        x.level_description AS experience_level,
        t.type_description AS employment_type,
        d.job_title, d.salary, d.salary_currency,
        d.salary_in_usd, d.remote_ratio, 
        d.employee_residence,
        i.name AS employee_residence_country,
        d.company_location,
        i2.name AS company_location_country,
        s.size_description AS company_size
    FROM ds_salaries AS d
    LEFT JOIN iso3166 AS i
        ON d.employee_residence = i.alpha_2
    LEFT JOIN iso3166 AS i2
        ON d.company_location = i2.alpha_2
    LEFT JOIN experience_levels AS x
        ON d.experience_level = x.level_code
    LEFT JOIN employment_types AS t
        ON d.employment_type = t.type_code
    LEFT JOIN company_size AS s
        ON d.company_size = s.size_code;

-- Finding employees whose residence is not in the US but their company location is in US
CREATE VIEW v_us_offshore_employees AS
    (SELECT 
        company_location_country, employee_residence_country,
        job_title, salary_in_usd, remote_ratio, 
        employment_type, experience_level
     FROM v_ds_salaries_nice
     WHERE employee_residence != 'US')
    EXCEPT
    (SELECT 
        company_location_country, employee_residence_country,
        job_title, salary_in_usd, remote_ratio, 
        employment_type, experience_level
     FROM v_ds_salaries_nice
     WHERE company_location != 'US')
    ORDER BY salary_in_usd ASC;

SELECT * FROM v_us_offshore_employees;