import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col

def main(session: snowpark.Session): 
    # Your code goes here, inside the "main" handler.
    tableName = 'INFLATION_INFO'
    df = session.table(tableName)
    
    df = df.unpivot("INFLATION_PERC", "YEAR", df.columns[4:66]).sort(["COUNTRY_NAME", "YEAR"])

    SF_countries = ["Netherlands","United Kingdom", "Italy", "Sweden", "Poland"]
    
    df_selected = df.select(col("COUNTRY_NAME"), col("YEAR"), col("INFLATION_PERC")).filter(col("COUNTRY_NAME").isin(SF_countries))
    # Print a sample of the dataframe to standard output.
    df_selected.show()

    df_selected.write.mode("overwrite").save_as_table("CLEANED_INFLATION_SNOWPARK_DATAFRAME")

    # Return value will appear in the Results tab.
    return df_selected