-- STEP 4: Making a file format
-- When loading data into a snowflake table, we have to define it's format, CSV, Parquet, JSON etc
-- We can create our own file format, whoch we then can easily re-use. Within this file format we can specify all kinds of things 


CREATE OR REPLACE FILE FORMAT csv
TYPE = 'CSV' -- data type
SKIP_HEADER = 1 -- skip the first row, since this contains the column names
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
-- STEP 5: Copy data from external stage into a snowflake table
-- 5.1 Have a look at the data

select

    $1 ::varchar,
    $2 ::varchar,
    $3 ::varchar,
    $4 ::varchar,
    $5 ::varchar,
    $6 ::int,
    $7 ::date, 
    $8 ::date, 
    $9 ::date,
    $10 ::date
    
from @AZUREBLOB/DimDiscount
(file_format => 'csv');


-- 5.2 make a table to copy in the data 
create or replace table DATA_ACADEMY.SNEAKER_FACTORY.Dim_Discount(
    DiscountID varchar,
    Promotion varchar, 
    DiscountType varchar,
    DiscountOn varchar,
    DiscountLevel varchar,
    Discount int,
    StartDate date,
    EndDate date,
    ValidFrom date,
    ValidTo date
)
; 
-- 5.3 copy data into the table

copy into DATA_ACADEMY.SNEAKER_FACTORY.Dim_Discount
from (
select
    $1 ::varchar,
    $2 ::varchar,
    $3 ::varchar,
    $4 ::varchar,
    $5 ::varchar,
    $6 ::int,
    $7 ::date,
    $8 ::date, 
    $9 ::date, 
    $10 ::date
from @AZUREBLOB/DimDiscount
(file_format => 'csv')
)

; 

select
    *
from DATA_ACADEMY.SNEAKER_FACTORY.Dim_Discount
;

-- STEP 6: 
-- Do the same for the text tables ; 
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
from @AZUREBLOB/DimCustomer
(file_format => 'txt')
;

create or replace table DATA_ACADEMY.SNEAKER_FACTORY.Dim_Customer(

    CustomerID varchar,
    Gender varchar,
    FirstName varchar,
    LastName varchar,
    StreetAddress varchar,
    City varchar,
    ZipCode varchar,
    CountryCode varchar,
    Country varchar,
    EmailAddress varchar,
    TelephoneNumber varchar,
    TelephoneCountryCode varchar,
    Birthday date,
    Latitude float,
    Longitude float

)
;
copy into DATA_ACADEMY.SNEAKER_FACTORY.Dim_Customer
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
from @AZUREBLOB/DimCustomer
(file_format => 'txt')
)
; 
select
*
from DATA_ACADEMY.SNEAKER_FACTORY.Dim_Customer
;

CREATE OR REPLACE TABLE DIM_STORE(
    StoreID varchar
    , StoreName varchar
    , City varchar
    , Address varchar
    , ZipCode varchar
    , CountryCode varchar
    , Country varchar
    , Link varchar
    , Channel varchar
)
;

COPY INTO DIM_STORE
FROM(
SELECT 
    $1::varchar
    , $2::varchar
    , $3::varchar
    , $4::varchar
    , $5::varchar
    , $6::varchar
    , $7::varchar
    , $8::varchar
    , $9::varchar

FROM @AZUREBLOB/DimStore
(file_format => 'txt')
)

;


CREATE OR REPLACE TABLE DIM_PRODUCT(
    ProductID varchar
    , ProductName varchar
    , ProductCategory varchar
    , ProductType varchar
    , ProductColor varchar
    , UnitPrice number(38,2)
    , DateIntroduced date
    , PriceGBP number(38,2)
    , PriceSEK number(38,2)
    , PricePLN number(38,2)
    , ProductSize integer
)
;

COPY INTO DIM_PRODUCT
FROM(
SELECT 
    $1::varchar
    , $2::varchar
    , $3::varchar
    , $4::varchar
    , $5::varchar
    , $6::number(38,2)
    , $7::date
    , $8::number(38,2)
    , $9::number(38,2)
    , $10::number(38,2)
    , $11::integer
FROM @AZUREBLOB/DimProduct
(file_format => 'txt')
)
;


CREATE OR REPLACE TABLE DIM_EMPLOYEE(
      EmployeeID varchar
    , Gender varchar
    , FirstName varchar
    , LastName varchar
    , StreetAddress varchar
    , City varchar
    , "STATE/PROVINCE" varchar
    , ZipCode varchar
    , CountryCode varchar
    , Country varchar
    , EmailAddress varchar
    , Telephonenumber varchar
    , TelephoneCountryCode varchar
    , Birthday date
    , Function varchar
    , Salary number(38,2)
    , StartDate date
    , EndDate date
    , IsActive boolean
    , Currency varchar
)
;

COPY INTO DIM_EMPLOYEE
FROM(
SELECT 
    $1::varchar
    , $2::varchar
    , $3::varchar
    , $4::varchar
    , $5::varchar
    , $6::varchar
    , $7::varchar
    , $8::varchar
    , $9::varchar
    , $10::varchar
    , $11::varchar
    , $12::varchar
    , $13::varchar
    , $14::date
    , $15::varchar
    , $16::number(38,2)
    , $17::date
    , $18::date
    , $19::boolean
    , $20::varchar
FROM @AZUREBLOB/DimEmployee
(file_format => 'txt')
);





CREATE OR REPLACE TABLE FACT_ORDERLINES(
    OrderLineID varchar
    , OrderID varchar
    , OrderDate date
    , CustomerID varchar
    , ProductID varchar
    , UnitPrice number(38,2)
    , CurrencyID varchar
    , Quantity integer
    , DiscountID varchar
    , DiscountAmount number(38,2)
    , NetPrice number(38,2)
    , Timestamp time
    , EmployeeID varchar
    , PaymentID varchar
    , StoreID varchar
)
;

COPY INTO FACT_ORDERLINES
FROM(
SELECT 
    $1::varchar
    , $2::varchar
    , $3::date
    , $4::varchar
    , $5::varchar
    , $6::number(38,2)
    , $7::varchar
    , $8::integer
    , $9::varchar
    , $10::number(38,2)
    , $11::number(38,2)
    , $12::time
    , $13::varchar
    , $14::varchar
    , $15::varchar

FROM @AZUREBLOB/FactOrderLines
(file_format => 'txt')
)
;


CREATE OR REPLACE TABLE FACT_RETURNS(
    ReturnID varchar
    , OrderID varchar
    , CustomerID varchar
    , ProductID varchar
    , UnitPrice number(38,2)
    , CurrencyID varchar
    , Quantity integer
    , DiscountID varchar
    , DiscountAmount number(38,2)
    , ReturnAmount number(38,2)
    , Timestamp time
    , EmployeeID varchar
    , PaymentID varchar
    , StoreID varchar
    , ReturnDate date
    , ReceivedDate date
)
;

COPY INTO FACT_RETURNS
FROM(
SELECT
    $1::varchar
    , $2::varchar
    , $3::varchar
    , $4::varchar
    , $5::number(38,2)
    , $6::varchar
    , $7::integer
    , $8::varchar
    , $9::number(38,2)
    , $10::number(38,2)
    , $11::time
    , $12::varchar
    , $13::varchar
    , $14::varchar
    , $15::date
    , $16::date
FROM @AZUREBLOB/FactReturns
(file_format => 'txt')
);
