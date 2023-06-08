--========== CONNECT SNOWFLAKE WITH AZUE BLON STORAGE AND LOAD DATA INTO SNOWFLAKE ====================

-- This worksheet will guide you through the process to connect snowflake with an azure blon storage. 
-- STEP 1: Create an storage integration 
-- A storage integration in Snowflake is a connection to an external storage service, such as Amazon S3, Microsoft Azure Blob Storage, or Google Cloud Storage. 
-- The storage integration allows Snowflake to access and manage data stored in the external storage service as if it were stored in Snowflake. 


create or replace database Snowflake_Burst;
create or replace schema Raw_Data;


create or replace storage integration azureblobstorage_integration
type = external_stage -- type of integration
storage_provider = azure -- provider of the storage
enabled = true
azure_tenant_id = '706234d5-b966-4413-9a48-d643f6e163b1'
storage_allowed_locations = ('azure://snowflakedata1234.blob.core.windows.net/snowflake-data-academy')

;

-- the location of the container within azure where our data is stored

desc integration azureblobstorage_integration; 
-- STEP 2: Making a database and schema for our data 

-- STEP 3: Create an external stage
-- An external stage in Snowflake is a named external location that you can use to read and write data to and from an external 
-- storage service, such as Amazon S3, Microsoft Azure Blob Storage, or Google Cloud Storage. The external stage is a way to acces data that is stored in an external storage
-- Think of it as a bridge between Snowflake and the external data stored in the external storage. 

create or replace stage azureblob
url = 'azure://snowflakedata1234.blob.core.windows.net/snowflake-data-academy' -- the location of the container within azure where our data is stored
credentials=(azure_sas_token='?sv=2022-11-02&ss=bfqt&srt=co&sp=rwdlacupiytfx&se=2023-06-06T19:14:29Z&st=2023-06-06T11:14:29Z&spr=https&sig=6uxfzhUHlGSqfattpUFzvC54%2F3WCGKxTrbY1EwSSoQQ%3D')
-- SAS token, which are the credentials needed to connect to the azure container in the external storage. 
;

-- With the external stage we have acces to the data in the external storage (azure blob). With this command we can have a look at what is in the external storage. 
-- We can now start reading reading data from this external stage and write it into a snowflake table. 
list @azureblob
; 
-- STEP 4: Making a file format
-- When loading data into a snowflake table, we have to define it's format, CSV, Parquet, JSON etc
-- We can create our own file format, whoch we then can easily re-use. Within this file format we can specify all kinds of things 


