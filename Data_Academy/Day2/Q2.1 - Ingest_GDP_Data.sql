CREATE OR REPLACE FILE FORMAT csv
TYPE = 'CSV' -- data type
SKIP_HEADER = 5 -- skip the first row, since this contains the column names
SKIP_BLANK_LINES = True
FIELD_DELIMITER = ','
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
ESCAPE_UNENCLOSED_FIELD = '\\'

;

create or replace table SNOWFLAKE_BURST.RAW_DATA.Raw_GDP_Data(
    Country_Name            varchar
    , Country_Code          varchar
    , Indicator_Name        varchar
    , year_2000             varchar
    , year_2001             varchar 
    , year_2002             varchar 
    , year_2003             varchar 
    , year_2004             varchar 
    , year_2005             varchar 
    , year_2006             varchar 
    , year_2007             varchar 	
    , year_2008             varchar 
    , year_2009             varchar 
    , year_2010             varchar 
    , year_2011             varchar 
    , year_2012             varchar 
    , year_2013             varchar 
    , year_2014             varchar 
    , year_2015             varchar 
    , year_2016             varchar 
    , year_2017             varchar 
    , year_2018             varchar 
    , year_2019             varchar 
    , year_2020             varchar 
    , year_2021             varchar
)
;


copy into SNOWFLAKE_BURST.RAW_DATA.Raw_GDP_Data
from(
select
      $1 
    , $2 
    , $3 
    , $45 
    , $46
    , $47
    , $48
    , $49
    , $50
    , $51
    , $52
    , $53
    , $54
    , $55
    , $56
    , $57
    , $58
    , $59
    , $60
    , $61
    , $62
    , $63
    , $64
    , $65
    , $66
from @AZUREBLOB/GDP_Data
(file_format => 'csv')
)

;
select 
    *
from SNOWFLAKE_BURST.RAW_DATA.Raw_GDP_Data
;


