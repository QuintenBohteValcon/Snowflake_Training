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
    "schema": "RAW_SNEAKER_FAQTORY_DATA",  # optional
    }

    New_Session = Session.builder.configs(connection_parameters).create()

    return New_Session

def Transformations(df, country_list):

    df_pivot = df.unpivot("Value", "Year", df.columns[3:25]).filter(fc.in_([df.col("COUNTRY_NAME")], country_list))

    df_pivot = df_pivot.select(df_pivot.col("COUNTRY_NAME"), \
                            df_pivot.col("Indicator_Name"), \
                            df_pivot.columns()
                            df_pivot.col("Value").cast(tp.DecimalType(15,2)).name("Value"), \
                            df_pivot.col("Year").substr(6,4).name("Year"))

    return df_pivot

def Write_df_to_SF(df, Table_Name):

    df.write.mode("overwrite").save_as_table(Table_Name)


#%%
New_Session = create_session()

df = New_Session.table("Raw_GDP_Data")

country_list = ["Netherlands", 'Poland', 'Italy']

df = Transformations(df, country_list)

Write_df_to_SF(df, 'GDP_Data')

