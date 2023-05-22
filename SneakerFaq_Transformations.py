
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
    "schema": "RAW_DATA",  # optional
    }

    New_Session = Session.builder.configs(connection_parameters).create()

    return New_Session


#%%

New_Session = create_session()
#%%

df = New_Session.table("VBRP")

#%%

def count_null_values():
    total_number_of_rows = df.count()

    df_null_count = df.select(total_number_of_rows - fc.count(df.col("POSNR")), 
                total_number_of_rows - fc.count(df.col("VBELN")),  
                total_number_of_rows - fc.count(df.col("ORDDT")), 
                total_number_of_rows - fc.count(df.col("MANDT")), 
                total_number_of_rows - fc.count(df.col("MATNR")), 
                total_number_of_rows - fc.count(df.col("STKPR")),
                total_number_of_rows - fc.count(df.col("WAERS")),
                total_number_of_rows - fc.count(df.col("FKIMG")),
                total_number_of_rows - fc.count(df.col("KPEIN")),
                total_number_of_rows - fc.count(df.col("TIMESTAMP")),
                total_number_of_rows - fc.count(df.col("PERNR")),
                total_number_of_rows - fc.count(df.col("BEZNR")),
                total_number_of_rows - fc.count(df.col("RSNR"))
                    )

    df_null_count.show()

def Transformations():

    df = df.select(df.col("POSNR").name("Orderline_ID"), 
                df.col("VBELN").name("Order_ID"),  
                df.col("ORDDT").cast(tp.DateType()).name("Order_Date"), 
                df.col("MATNR").name("Product_ID"), 
                df.col("STKPR").cast(tp.DecimalType(4,2)).name("Unit_Price"),
                df.col("WAERS").name("Currency"),
                df.col("FKIMG").cast(tp.IntegerType()).name("Quantity"),
                df.col("KPEIN").name("Discount"),
                df.col("TIMESTAMP").name("Timestamp"),
                df.col("PERNR").name("Employee_ID"),
                df.col("BEZNR").name("Payment_ID"),
                df.col("RSNR").name("Store_ID")
    )

    df = df.dropna()

    return df


def Write_df_to_SF(df, Table_Name):

    df.write.mode("overwrite").save_as_table(Table_Name)


#%%

# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col
from snowflake.snowpark import Session
from snowflake.snowpark import DataFrame
from snowflake.snowpark import functions as fc
from snowflake.snowpark import Column as cl
from snowflake.snowpark import Table
from snowflake.snowpark import Row
from snowflake.snowpark import DataFrameWriter
from snowflake.snowpark import types as tp


def count_null_values(df):
    total_number_of_rows = df.count()

    df_null_count = df.select(total_number_of_rows - fc.count(df.col("POSNR")), 
                total_number_of_rows - fc.count(df.col("VBELN")),  
                total_number_of_rows - fc.count(df.col("ORDDT")), 
                total_number_of_rows - fc.count(df.col("MANDT")), 
                total_number_of_rows - fc.count(df.col("MATNR")), 
                total_number_of_rows - fc.count(df.col("STKPR")),
                total_number_of_rows - fc.count(df.col("WAERS")),
                total_number_of_rows - fc.count(df.col("FKIMG")),
                total_number_of_rows - fc.count(df.col("KPEIN")),
                total_number_of_rows - fc.count(df.col("TIMESTAMP")),
                total_number_of_rows - fc.count(df.col("PERNR")),
                total_number_of_rows - fc.count(df.col("BEZNR")),
                total_number_of_rows - fc.count(df.col("RSNR"))
                    )

    df_null_count.show()

def Transformations(df):

    df = df.select(df.col("POSNR").name("Orderline_ID"), 
                df.col("VBELN").name("Order_ID"),  
                df.col("ORDDT").cast(tp.DateType()).name("Order_Date"), 
                df.col("MATNR").name("Product_ID"), 
                df.col("STKPR").cast(tp.DecimalType(4,2)).name("Unit_Price"),
                df.col("WAERS").name("Currency"),
                df.col("FKIMG").cast(tp.IntegerType()).name("Quantity"),
                df.col("KPEIN").name("Discount"),
                df.col("TIMESTAMP").name("Timestamp"),
                df.col("PERNR").name("Employee_ID"),
                df.col("BEZNR").name("Payment_ID"),
                df.col("RSNR").name("Store_ID")
    )

    df = df.dropna()

    return df


def Write_df_to_SF(df, Table_Name):

    df.write.mode("overwrite").save_as_table(Table_Name)

def main(session: snowpark.Session): 
    # Your code goes here, inside the "main" handler.

    dataframe = session.table('SNOWFLAKE_BURST.RAW_DATA.VBRP')
    
    count_null_values(dataframe)

    df = Transformations(dataframe)

    Table_Name = 'FactOrderlines'
    
    Write_df_to_SF(df, Table_Name)
