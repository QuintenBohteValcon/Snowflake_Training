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

    GDP_up = GDP.unpivot("Value", "Year", GDP.columns[3:25]).filter(fc.in_([GDP.col("COUNTRY_NAME")], country_list))

    GDP_up = GDP_up.select(GDP_up.col("COUNTRY_NAME"), \
                            GDP_up.col("Indicator_Name"), \
                            GDP_up.col("Value").cast(tp.DecimalType(15,2)).name("Value"), \
                            GDP_up.col("Year").substr(6,4).name("Year"))





    df_pivot = df_pivot.with_column("Value_Euro", )

    return df_pivot



def Write_df_to_SF(df, Table_Name):

    df.write.mode("overwrite").save_as_table(Table_Name)


#%%
New_Session = create_session()

GDP = New_Session.table("Raw_GDP_Data")
Currency = New_Session.table("FactCurrencyRates")

Currency.show()
#%%

country_list = ["Netherlands", 'Poland', 'Italy']


GDP_up = GDP.unpivot("Value", "Year", GDP.columns[3:25]).filter(fc.in_([GDP.col("COUNTRY_NAME")], country_list))


GDP_up = GDP_up.select(GDP_up.col("COUNTRY_NAME"), \
                        GDP_up.col("Indicator_Name"), \
                        GDP_up.col("Value").cast(tp.DecimalType(15,2)).name("Value"), \
                        GDP_up.col("Year").substr(6,4).name("Year"))


Currency_year = Currency.select(Currency.YEARMONTHID).cast()

group_by(fc.year(Currency.col("Year")).name("year"), \
                                  Currency.CURRENCYID,)  \
                        .avg(fc.sum(Currency.col("CURRENCYRATE")).name("CURRENCYRATE"))


Currency_year.show()

#%%


df_pivot = df_pivot.with_column("Value_Euro", )

df = Transformations(df, country_list)

Write_df_to_SF(df, 'GDP_Data')



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



def Transformations(df, country_list):
    
    df = df.unpivot("Value", "Year", df.columns[3:25]).filter(fc.in_([df.col("COUNTRY_NAME")], country_list))

    df = df.select(df.col("COUNTRY_NAME"), \
                            df.col("Indicator_Name"), \
                            df.col("Value").cast(tp.DecimalType(15,2)).name("Value"), \
                            df.col("Year").substr(6,4).name("Year"))
    return df



def Write_df_to_SF(df, Table_Name):

    df.write.mode("overwrite").save_as_table(Table_Name)


def main(session: snowpark.Session): 
    
    dataframe = session.table('SNOWFLAKE_BURST.RAW_DATA.RAW_GDP_DATA')

    dataframe.show()
    
    country_list = ["Netherlands", 'Poland', 'Italy']
    
    df = Transformations(dataframe, country_list)

    df.show()
    
    Write_df_to_SF(df , 'GDP_Data')



