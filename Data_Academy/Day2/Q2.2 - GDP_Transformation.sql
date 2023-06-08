CREATE OR REPLACE TABLE SNOWFLAKE_BURST.CLEAN_DATA.GDP_Data_sql
AS
WITH CTE AS 
(
SELECT 

    country_name
    , "YEAR_2000" ,"YEAR_2001","YEAR_2002","YEAR_2003",
    "YEAR_2004","YEAR_2005","YEAR_2006", "YEAR_2007",
    "YEAR_2008","YEAR_2009","YEAR_2010", "YEAR_2011",
    "YEAR_2012","YEAR_2013","YEAR_2014", "YEAR_2015",
    "YEAR_2016","YEAR_2017","YEAR_2018", "YEAR_2019",
    "YEAR_2020","YEAR_2021"
    
FROM SNOWFLAKE_BURST.RAW_DATA.RAW_GDP_DATA
WHERE COUNTRY_NAME IN ('Netherlands','United Kingdom', 'Italy','Sweden', 'Poland')
)
, unpivot_gdp as
(
SELECT 

    Country_Name            as Country_Name
    , right(year,4)         as year 
    , GDP                   as GDP
    
FROM CTE
UNPIVOT( GDP for YEAR in 
    ("YEAR_2000" ,"YEAR_2001","YEAR_2002","YEAR_2003",
    "YEAR_2004","YEAR_2005","YEAR_2006", "YEAR_2007",
    "YEAR_2008","YEAR_2009","YEAR_2010", "YEAR_2011",
    "YEAR_2012","YEAR_2013","YEAR_2014", "YEAR_2015",
    "YEAR_2016","YEAR_2017","YEAR_2018", "YEAR_2019",
    "YEAR_2020","YEAR_2021"))
ORDER BY COUNTRY_NAME, YEAR

)

, Currency_Rates as
(
select 

    left(yearmonthid, 4)         as year
    , avg(currencyrate)          as currency_rate
    
from SNOWFLAKE_BURST.RAW_DATA.FACTCURRENCYRATES
where currencyid = 'USD'
group by
    year
order by
    year
)

, convert_euro_to_usd as
(
select 

    gdp.Country_Name                                     as country_name
    , gdp.year                                           as year
    , to_number((gdp.GDP / cr.currency_rate), 38,2)      as GDP_Euro
    , 'EUR'                                              as Currency
    
from unpivot_gdp as gdp
left join Currency_Rates as cr
    on gdp.year = cr.year
)

select
    country_name
    , year
    , GDP_Euro
    , Currency
from convert_euro_to_usd
;

