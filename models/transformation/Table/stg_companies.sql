with entreprises as (
    select *
    from companies
)


SELECT
    properties_name as company,
    properties_domain as domain,
    properties_city as city,
    properties_country as country,
    properties_phone as phone_number,
    properties_industry as industry,
    properties_numberofemployees as nb_employee,
    CAST(_airbyte_normalized_at as TIMESTAMP) as implementation_date
FROM entreprises

