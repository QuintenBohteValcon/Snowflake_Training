#%%
from snowflake.snowpark import Session
from snowflake.snowpark import DataFrame
from snowflake.snowpark import functions as fc
from snowflake.snowpark import Column as cl
from snowflake.snowpark import Table
from snowflake.snowpark import DataFrameWriter
from snowflake.snowpark import types as tp
import pandas as pd
import numpy as np
import os

#%%

def create_session():

    connection_parameters = {
    "account": "PT19212.west-europe.azure",
    "user": "Quintenbohte",
    "password": "6HtsyM@12345",
    "role": "accountadmin",  # optional
    "warehouse": "COMPUTE_WH",  # optional
    "database": "SNOWFLAKE_BURST",  # optional
    "schema": "RAW_DATA",  # optional
    }

    New_Session = Session.builder.configs(connection_parameters).create()

    return New_Session

def Transformations(GDP, Currency, country_list):


    #Unpivot the GDP table
    GDP_up = GDP.unpivot("Value", "Year", GDP.columns[3:25]).filter(fc.in_([GDP.col("COUNTRY_NAME")], country_list))

    #select all necessary columns
    GDP_up = GDP_up.select(GDP_up.col("COUNTRY_NAME"), \
                            GDP_up.col("Indicator_Name"), \
                            GDP_up.col("Value").cast(tp.DecimalType(15,2)).name("Value"), \
                            GDP_up.col("Year").substr(6,4).name("Year"))
    
    #select all columns + select only year from yearmonthid for group by
    Currency_year = Currency.select(fc.left(Currency.YEARMONTHID.cast(tp.StringType()), 4).name("Year"), 
                                Currency.CurrencyID,
                                Currency.CurrencyRate).filter(Currency.CurrencyID == "USD")
                        

    #group by year and compute average currency rate per year
    Currency_year = Currency_year.group_by(Currency_year.year, 
                                        Currency_year.CurrencyID) \
                                .agg(fc.avg(Currency_year.CurrencyRate).name("CurrencyRate")).order_by(Currency_year.year)

    #join currency year on GDP_Data and convert dollars to euro's
    GDP_Data = GDP_up.join(Currency_year, 
                        GDP_up.year == Currency_year.year)\
                    .select(GDP_up.year.name("Year"),
                            GDP_up.country_name.name("Country"),
                            GDP_up.value,
                            Currency_year.CurrencyRate,
                            fc.lit("EUR").name('Currency')
                    )\
                    .withColumn('Value_Euro', GDP_up.value / Currency_year.CurrencyRate)
    
    #select columns
    GDP_Data = GDP_Data.select(GDP_Data.year,
                            GDP_Data.country,
                            GDP_Data.Value_Euro,
                            GDP_Data.Currency)

    return GDP_Data

def Write_df_to_SF(df, Table_Name):

    df.write.mode("overwrite").save_as_table(Table_Name)


#%%
New_Session = create_session()
GDP = New_Session.table("Raw_GDP_Data")
Currency = New_Session.table("FactCurrencyRates")
country_list = ["Netherlands", 'Poland', 'Italy', 'Sweden', 'United Kingdom']

GDP_Data = Transformations(GDP, Currency, country_list)

Write_df_to_SF(GDP_Data, 'GDP_Data')



#%%

# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark import Session
from snowflake.snowpark import DataFrame
from snowflake.snowpark import functions as fc
from snowflake.snowpark import Column as cl
from snowflake.snowpark import Table
from snowflake.snowpark import DataFrameWriter
from snowflake.snowpark import types as tp
from snowflake.snowpark.functions import col


def Transformations(GDP, Currency , country_list):
    
    #Unpivot the GDP table
    GDP_up = GDP.unpivot("Value", "Year", GDP.columns[3:25]).filter(fc.in_([GDP.col("COUNTRY_NAME")], country_list))

    #select all necessary columns
    GDP_up = GDP_up.select(GDP_up.col("COUNTRY_NAME"), \
                            GDP_up.col("Indicator_Name"), \
                            GDP_up.col("Value").cast(tp.DecimalType(15,2)).name("Value"), \
                            GDP_up.col("Year").substr(6,4).name("Year"))
    
    #select all columns + select only year from yearmonthid for group by
    Currency_year = Currency.select(fc.left(Currency.YEARMONTHID.cast(tp.StringType()), 4).name("Year"), 
                                Currency.CurrencyID,
                                Currency.CurrencyRate).filter(Currency.CurrencyID == "USD")
                        

    #group by year and compute average currency rate per year
    Currency_year = Currency_year.group_by(Currency_year.year, 
                                        Currency_year.CurrencyID) \
                                .agg(fc.avg(Currency_year.CurrencyRate).name("CurrencyRate")).order_by(Currency_year.year)

    #join currency year on GDP_Data and convert dollars to euro's
    GDP_Data = GDP_up.join(Currency_year, 
                        GDP_up.year == Currency_year.year)\
                    .select(GDP_up.year.name("Year"),
                            GDP_up.country_name.name("Country"),
                            GDP_up.value,
                            Currency_year.CurrencyRate,
                            fc.lit("EUR").name('Currency')
                    )\
                    .withColumn('Value_Euro', GDP_up.value / Currency_year.CurrencyRate)
    
    #select columns
    GDP_Data = GDP_Data.select(GDP_Data.year,
                            GDP_Data.country,
                            GDP_Data.Value_Euro,
                            GDP_Data.Currency)

    return GDP_Data


def Write_df_to_SF(df, Table_Name):

    df.write.mode("overwrite").save_as_table(Table_Name)


def main(session: snowpark.Session): 
    
    GDP = session.table("SNOWFLAKE_BURST.RAW_DATA.RAW_GDP_DATA")
    
    Currency = session.table("SNOWFLAKE_BURST.RAW_DATA.FACTCURRENCYRATES")
    
    country_list = ["Netherlands", 'Poland', 'Italy', 'Sweden', 'United Kingdom']
   
    GDP_Data = Transformations(GDP, Currency, country_list)

    GDP_Data.show()
    
    Write_df_to_SF(GDP_Data , 'GDP_Data')
