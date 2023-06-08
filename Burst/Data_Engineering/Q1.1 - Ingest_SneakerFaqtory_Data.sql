CREATE OR REPLACE FILE FORMAT csv
TYPE = 'CSV' -- data type
SKIP_HEADER = 2 -- skip the first row, since this contains the column names
FIELD_DELIMITER = ','
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
ESCAPE_UNENCLOSED_FIELD = '\\'
; 

CREATE OR REPLACE FILE FORMAT txt
TYPE = 'CSV' -- data type
SKIP_HEADER = 1 -- skip the first row, since this contains the column names
FIELD_DELIMITER = '\t'
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
ESCAPE_UNENCLOSED_FIELD = '\\'
;

CREATE OR REPLACE FILE FORMAT json
TYPE = 'JSON'
STRIP_OUTER_ARRAY = True
;

-- Ingest Fact Orderlines

create or replace table SNOWFLAKE_BURST.RAW_DATA.FactOrderLines(
      OrderLineID varchar	
    , OrderID varchar
    , OrderDate date
    , CustomerID varchar
    , ProductID varchar
    , UnitPrice number(38,2)
    , CurrencyID varchar
    , Quantity integer
    , DiscountID varchar
    , DiscountAmount varchar
    , NetPrice number(38,2)
    , Timestamp time
    , EmployeeID varchar
    , PaymentID varchar
    , StoreID varchar
)
; 



copy into SNOWFLAKE_BURST.RAW_DATA.FactOrderLines
from(
select
    $1 ::varchar,
    $2 ::varchar,
    $3 ::varchar,
    $4 ::varchar,
    $5 ::varchar,
    $6 ::varchar,
    $7 ::varchar, 
    $8 ::varchar,
    $9 ::varchar,
    $10 ::varchar,
    $11 ::varchar,
    $12 ::varchar,
    $13 ::varchar, 
    $14 ::varchar,
    $15 ::varchar
    
from @AZUREBLOB/FactOrderLines
(file_format => 'txt')
)
;

create or replace table SNOWFLAKE_BURST.RAW_DATA.DimStore(
    StoreID varchar
    , StoreName varchar
    , City varchar
    , Address varchar
    , Zip varchar
    , CountryCode varchar
    , Country varchar
    , Link varchar
    , Channel varchar
)
; 

copy into SNOWFLAKE_BURST.RAW_DATA.DimStore
from(
select
    $1 ::varchar,
    $2 ::varchar,
    $3 ::varchar,
    $4 ::varchar,
    $5 ::varchar,
    $6 ::varchar,
    $7 ::varchar, 
    $8 ::varchar,
    $9 ::varchar
    
from @AZUREBLOB/DimStore
(file_format => 'txt')
)

;


create or replace table SNOWFLAKE_BURST.RAW_DATA.FactCurrencyRates(
    CurrencyRateID varchar
    , YearMonthID varchar
    , CurrencyID varchar
    , CurrencyRate number(38,20)
)

;

copy into SNOWFLAKE_BURST.RAW_DATA.FactCurrencyRates
from(
select
    $1 ::varchar,
    $2 ::varchar,
    $3 ::varchar,
    $4 ::varchar
    
from @AZUREBLOB/FactCurrencyRates
(file_format => 'txt')
)
