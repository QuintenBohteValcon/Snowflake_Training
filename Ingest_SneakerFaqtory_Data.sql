
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

-- 5.2 make a table to copy in the data 
create or replace table SNOWFLAKE_BURST.RAW_DATA.VBRP(
    index varchar,
    POSNR varchar,
    VBELN varchar, 
    ORDDT varchar,
    MANDT varchar,
    MATNR varchar,
    STKPR varchar,
    WAERS varchar,
    FKIMG varchar,
    SKTOF varchar,
    KPEIN varchar,
    Timestamp varchar,
    PERNR varchar,
    BEZNR varchar,
    RSNR varchar
)
; 



copy into SNOWFLAKE_BURST.RAW_DATA.VBRP
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
    
from @AZUREBLOB/dsl.VBRP
(file_format => 'csv')
)

;

select 
    *
from SNOWFLAKE_BURST.RAW_DATA.VBRP
;