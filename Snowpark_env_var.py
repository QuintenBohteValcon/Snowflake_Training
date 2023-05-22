#%%

from snowflake.snowpark import Session
from snowflake.snowpark import DataFrame
import pandas as pd
import numpy as np
import os
new_session = Session.builder.configs(connection_parameters1).create()
#%%
df_pandas = DataFrame.to_pandas(new_session.table("dim_discount"))
df_pandas.head()
#%%
