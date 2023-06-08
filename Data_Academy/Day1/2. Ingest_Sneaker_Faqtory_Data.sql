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

;
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
-- 5.3 cop data into the table

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

