#%%
from snowflake.snowpark import Session
from snowflake.snowpark import DataFrame
from snowflake.snowpark import functions as fc
from snowflake.snowpark import Column as cl
from snowflake.snowpark import Table
from snowflake.snowpark import Row
from snowflake.snowpark import CaseExpr as cw
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


def Transformations(Orderlines, Currency, Stores):

    #Select necesarry columns + create YearMonth_ID column
    Orderlines = Orderlines.select(Orderlines.col("StoreID").name("Store_ID"), \
                            fc.year(Orderlines.col("OrderDate")).name("Year"), \
                            Orderlines.col("UnitPrice"), \
                            Orderlines.col("CurrencyID").name("Currency_ID"), \
                            fc.left(Orderlines.col('OrderDate').cast(tp.StringType()), 7).name('YearMonth_ID'))

    #Join Currency table + store table
    Orderlines = Orderlines.join(Currency, \
                                (Orderlines.col("YearMonth_ID") == Currency.col("YEARMONTHID")) & \
                                (Orderlines.col("Currency_ID") == Currency.col("CurrencyID")),  how = "left" )

    Orderlines = Orderlines.join(Stores, Orderlines.col("STORE_ID") == Stores.col("STOREID"), how = "left" )

    #convert currencies to euro
    Orderlines = Orderlines.with_column('UnitPriceEuro',\
                            fc.when(Orderlines.col("Currency_ID") == 'EUR', Orderlines.col("UnitPrice"))
                            .otherwise((Orderlines.col("UnitPrice") / Orderlines.col("CurrencyRate"))))
        
    #Group by 
    Orderlines = Orderlines.group_by(Orderlines.col("Year"), \
                                    Orderlines.col("Store_ID"), \
                                    Orderlines.col("Country"))\
                            .agg(fc.sum(Orderlines.col("UnitPriceEuro")).name("Sales"))

    #select final columns
    Orderlines = Orderlines.select(Orderlines.Year, \
                                Orderlines.Country, \
                                    Orderlines.Sales, \
                                    fc.lit("EUR").name("Currency"))\
                            .order_by(Orderlines.Country, Orderlines.Year)
    return Orderlines

def Write_df_to_SF(df, Table_Name):

    df.write.mode("overwrite").save_as_table(Table_Name)



#%%
New_Session = create_session()
Orderlines = New_Session.table("FACTORDERLINES")
Currency = New_Session.table("FactCurrencyRates")
Stores = New_Session.table("DimStore")

Sales = Transformations(Orderlines, Currency, Stores)
Write_df_to_SF(Sales, "Sales_Over_Time")




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

def Transformations(Orderlines, Currency, Stores):
    
    #Select necesarry columns + create YearMonth_ID column
    Orderlines = Orderlines.select(Orderlines.col("StoreID").name("Store_ID"), \
                            fc.year(Orderlines.col("OrderDate")).name("Year"), \
                            Orderlines.col("UnitPrice"), \
                            Orderlines.col("CurrencyID").name("Currency_ID"), \
                            fc.left(Orderlines.col('OrderDate').cast(tp.StringType()), 7).name('YearMonth_ID'))

    #Join Currency table + store table
    Orderlines = Orderlines.join(Currency, \
                                (Orderlines.col("YearMonth_ID") == Currency.col("YEARMONTHID")) & \
                                (Orderlines.col("Currency_ID") == Currency.col("CurrencyID")),  how = "left" )

    Orderlines = Orderlines.join(Stores, Orderlines.col("STORE_ID") == Stores.col("STOREID"), how = "left" )

    #convert currencies to euro
    Orderlines = Orderlines.with_column('UnitPriceEuro',\
                            fc.when(Orderlines.col("Currency_ID") == 'EUR', Orderlines.col("UnitPrice"))
                            .otherwise((Orderlines.col("UnitPrice") / Orderlines.col("CurrencyRate"))))
        
    #Group by 
    Orderlines = Orderlines.group_by(Orderlines.col("Year"), \
                                    Orderlines.col("Store_ID"), \
                                    Orderlines.col("Country"))\
                            .agg(fc.sum(Orderlines.col("UnitPriceEuro")).name("Sales"))

    #select final columns
    Orderlines = Orderlines.select(Orderlines.Year, \
                                Orderlines.Country, \
                                    Orderlines.Sales, \
                                    fc.lit("EUR").name("Currency"))\
                            .order_by(Orderlines.Country, Orderlines.Year)
    
    return Orderlines

def Write_df_to_SF(df, Table_Name):
    df.write.mode("overwrite").save_as_table(Table_Name)


def main(session: snowpark.Session): 

    Orderlines = session.table("FACTORDERLINES")

    Currency = session.table("FactCurrencyRates")
    
    Stores = session.table("DimStore")
    
    Orderlines = Transformations(Orderlines, Currency, Stores)
    
    Orderlines.show()

    Write_df_to_SF(Orderlines, "Sales_Over_Time")
    