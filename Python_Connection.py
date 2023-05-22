#%%
from snowflake.snowpark import Session
from snowflake.snowpark import DataFrame
from snowflake.snowpark import functions as fc
from snowflake.snowpark import Table
from snowflake.snowpark import DataFrameWriter
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

#%%
New_Session = create_session()

#%%

df = New_Session.table("Raw_GDP_Data")
#%%

#%%
count = 0

year_column = []

year = 1999

for i in range(1, 23):

    year =+  year + 1

    year_column.append(year)
    
year_df = New_Session.create_dataframe(year_column, schema=["year"])

column = year_df.select(year_df.col("year"))


#%%
year_df1 = year_df.with_column('year1', year_df["year"])

year_df1.show()
#%%

country_list = ["Netherlands", 'Poland', 'Italy']

count = 0

for country in country_list:

    count += 1

    df_pivot = df.filter((df.col("COUNTRY_NAME") == country)).unpivot("Value", "Year", df.columns[3:25])

    df_pivot = df_pivot.select(df_pivot.col('Country_Name'), df_pivot.col('Indicator_Name'), df_pivot.col('Value'))

    df_pivot = df_pivot.with_column('year', fc.lit('test'))
    
    if count == 1:
    
        GDP_Dataframe = df_pivot

    else:
        GDP_Dataframe = DataFrame.union_all(GDP_Dataframe, df_pivot)

#%%

GDP_DataFrame_pd = GDP_Dataframe.toPandas()

GDP_Dataframe.show()

#%%

GDP_Dataframe.write.mode("overwrite").save_as_table("GDP_Dataframe")

#%%




