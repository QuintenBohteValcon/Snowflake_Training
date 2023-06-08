create or replace table SNOWFLAKE_BURST.RAW_DATA.SALES_OVER_TIME
as
with orderlines as 
(
select 
    STOREID As Store_ID
    , year(orderdate) as Year
    , UNITPRICE as UnitPrice
    , CURRENCYID as CurrencyID
    , left(cast(orderdate as varchar), 7) as YearMonthID
from SNOWFLAKE_BURST.RAW_DATA.FACTORDERLINES
)

, Currency as
(
select 
    YEARMONTHID as YearMonthID
    , CURRENCYID as CurrencyID
    , CURRENCYRATE as CurrencyRate
from SNOWFLAKE_BURST.RAW_DATA.FACTCURRENCYRATES

)

, Store as
(
select 
    storeid as storeid
    , country as country
from SNOWFLAKE_BURST.RAW_DATA.DIMSTORE

)

, join_table as
(
select 
    ol.Year
    , str.country
    , ol.UnitPrice
    , ol.currencyid
    , case 
        when ol.CurrencyID = 'EUR' then ol.UnitPrice
        else ol.UnitPrice/cur.CurrencyRate 
        end as UnitPrice_Euro
    
from orderlines as ol
left join Currency as cur
    on ol.yearmonthid = cur.yearmonthid
    and ol.currencyid = cur.currencyid
left join store as str
    on ol.store_id = str.storeid
)

select 
    Year
    , country
    , sum(UnitPrice_Euro)
    , 'EUR' as Currency
from join_table
group by
    year
    , country
order by
    Country,
    year
