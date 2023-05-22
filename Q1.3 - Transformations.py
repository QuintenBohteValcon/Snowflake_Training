#%%
from snowflake.snowpark import Session
from snowflake.snowpark import DataFrame
from snowflake.snowpark import functions as fc
from snowflake.snowpark import Column as cl
from snowflake.snowpark import Table
from snowflake.snowpark import Row
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
    "schema": "CLEAN_DATA",  # optional
    }

    New_Session = Session.builder.configs(connection_parameters).create()

    return New_Session


def Transformations(df1, df2):
    Orderlines = df1
    Stores = df2
    Orderlines = OrL.select(OrL.col("Store_ID"), fc.year(OrL.col("Order_Date")).name("Year"), OrL.col("Unit_Price"), OrL.col("Currency"))
    Orderlines = Orderlines.group_by(Orderlines.col("Year"), Orderlines.col("Store_ID"), Orderlines.col("Currency")) \
                .agg(fc.sum(Orderlines.col("Unit_Price")).name("Amount")).filter((Orderlines.col("Store_ID")) == 'ST01')
    Orderlines = Orderlines.join(Stores, Orderlines.col("STORE_ID") == Stores.col("STOREID"), how = "left" )
    Orderlines = Orderlines.select(Orderlines.col("Year"), Orderlines.col("Country"), Orderlines.col("Currency"), Orderlines.col("Amount"))
    
    return Orderlines

def Write_df_to_SF(df, Table_Name):

    df.write.mode("overwrite").save_as_table(Table_Name)

#%%
New_Session = create_session()
OrL = New_Session.table("FACTORDERLINES")
Stores = New_Session.table("STORES")
#%%
Orderlines = Transformations(OrL, Stores)

Write_df_to_SF(Orderlines, "Sales_Over_Time")









# %%
