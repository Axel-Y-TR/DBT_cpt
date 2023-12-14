with entreprises as ( 
    SELECT *
    FROM {{ ref('stg_companies') }}
),
country_stats as (
    SELECT 
        country,
        count(DISTINCT(city)) as nb_city,
        count(DISTINCT(domain)) as nb_company,
        Round(AVG(nb_employee)) as avg_employee,
        count(DISTINCT(industry)) as nb_industries
    FROM entreprises 
    GROUP BY country
),
computer_stats as (
    SELECT 
        country,
        COUNT(*) AS nb_comp_COMPUTER_SOFTWARE,
        ROUND(AVG(nb_employee)) AS avg_emp_COMPUTER_SOFTWARE,
        MIN(nb_employee) AS min_emp_COMPUTER_SOFTWARE,
        MAX(nb_employee) AS max_emp_COMPUTER_SOFTWARE
    FROM entreprises
    WHERE industry = 'COMPUTER_SOFTWARE'
    GROUP BY country
),
estate_stats as (
    SELECT 
        country,
        COUNT(*) AS nb_comp_REAL_ESTATE,
        ROUND(AVG(nb_employee)) AS avg_emp_REAL_ESTATE,
        MIN(nb_employee) AS min_emp_REAL_ESTATE,
        MAX(nb_employee) AS max_emp_REAL_ESTATE
    FROM entreprises
    WHERE industry = 'REAL_ESTATE'
    GROUP BY country
),
elec_stats as (
    SELECT 
        country,
        COUNT(*) AS nb_comp_ELECTRICAL_ELECTRONIC_MANUFACTURING,
        ROUND(AVG(nb_employee)) AS avg_emp_ELECTRICAL_ELECTRONIC_MANUFACTURING,
        MIN(nb_employee) AS min_emp_ELECTRICAL_ELECTRONIC_MANUFACTURING,
        MAX(nb_employee) AS max_emp_ELECTRICAL_ELECTRONIC_MANUFACTURING
    FROM entreprises
    WHERE industry = 'ELECTRICAL_ELECTRONIC_MANUFACTURING'
    GROUP BY country
),
account_stats as (
    SELECT 
        country,
        COUNT(*) AS nb_comp_ACCOUNTING,
        ROUND(AVG(nb_employee)) AS avg_emp_ACCOUNTING,
        MIN(nb_employee) AS min_emp_ACCOUNTING,
        MAX(nb_employee) AS max_emp_ACCOUNTING
    FROM entreprises
    WHERE industry = 'ACCOUNTING'
    GROUP BY country
),
CE_stats as (
    SELECT 
        country,
        COUNT(*) AS nb_comp_CONSUMER_ELECTRONICS,
        ROUND(AVG(nb_employee)) AS avg_emp_CONSUMER_ELECTRONICS,
        MIN(nb_employee) AS min_emp_CONSUMER_ELECTRONICS,
        MAX(nb_employee) AS max_emp_CONSUMER_ELECTRONICS
    FROM entreprises 
    WHERE industry = 'CONSUMER_ELECTRONICS'
    GROUP BY country
)

SELECT 
  cs.country,
  cs.nb_city,
  cs.nb_company,
  cs.avg_employee,
  cs.nb_industries,
  comp.nb_comp_COMPUTER_SOFTWARE,
  comp.avg_emp_COMPUTER_SOFTWARE,
  comp.min_emp_COMPUTER_SOFTWARE,
  comp.max_emp_COMPUTER_SOFTWARE,
  es.nb_comp_REAL_ESTATE,
  es.avg_emp_REAL_ESTATE,
  es.min_emp_REAL_ESTATE,
  es.max_emp_REAL_ESTATE,
  el.nb_comp_ELECTRICAL_ELECTRONIC_MANUFACTURING,
  el.avg_emp_ELECTRICAL_ELECTRONIC_MANUFACTURING,
  el.min_emp_ELECTRICAL_ELECTRONIC_MANUFACTURING,
  el.max_emp_ELECTRICAL_ELECTRONIC_MANUFACTURING,
  acc.nb_comp_ACCOUNTING,
  acc.avg_emp_ACCOUNTING,
  acc.min_emp_ACCOUNTING,
  acc.max_emp_ACCOUNTING,
  ce.nb_comp_CONSUMER_ELECTRONICS,
  ce.avg_emp_CONSUMER_ELECTRONICS,
  ce.min_emp_CONSUMER_ELECTRONICS,
  ce.max_emp_CONSUMER_ELECTRONICS 
  
FROM 
    country_stats cs
LEFT JOIN 
    computer_stats comp ON cs.country = comp.country
LEFT JOIN 
    estate_stats es ON cs.country = es.country
LEFT JOIN 
    elec_stats el ON cs.country = el.country
LEFT JOIN 
    account_stats acc ON cs.country = acc.country
LEFT JOIN 
    CE_stats ce ON cs.country = ce.country








