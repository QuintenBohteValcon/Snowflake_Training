import snowflake.snowpark as snowpark
#from snowflake.snowpark.functions import col
import pandas as pd

def main(session: snowpark.Session): 
    # Your code goes here, inside the "main" handler.
    
    dataframe = session.table("Inflation_info")

    pandas_df = dataframe.to_pandas()

    # Print a sample of the dataframe to standard output.
    #dataframe.show()

    df_unpivot = pd.melt(pandas_df, id_vars=['COUNTRY_NAME'], value_vars=['1960','1960','1961','1962',
                                                                          '1963','1964','1965','1966',
                                                                          '1967','1968','1969','1970',
                                                                          '1971','1972','1973','1974',
                                                                          '1975','1976','1977','1978',
                                                                          '1979','1980','1981','1982',
                                                                          '1983','1984','1985','1986',
                                                                          '1987','1988','1989','1990',
                                                                          '1991','1992','1993','1994',
                                                                          '1995','1996','1997','1998',
                                                                          '1999','2000','2001','2002',
                                                                          '2003','2004','2005','2006',
                                                                          '2007','2008','2009','2010',
                                                                          '2011','2012','2013','2014',
                                                                          '2015','2016','2017','2018',
                                                                          '2019','2020','2021'], var_name = 'YEAR', value_name= 'INFLATION_PERC').sort_values(by=['COUNTRY_NAME','YEAR'])

   
    countries_list = ['United Kingdom', 'Netherlands', 'Sweden', 'Italy', 'Poland']

    df_filtered = df_unpivot.loc[df_unpivot['COUNTRY_NAME'].isin(countries_list)]
    df_filtered['INFLATION_PERC'] = df_filtered['INFLATION_PERC'].replace('',0).astype('float')
    df_filtered['YEAR'] = df_filtered['YEAR'].astype('int')

    #print(df_filtered)
    df_snowflake = session.create_dataframe(df_filtered)
    df_snowflake.write.mode("overwrite").save_as_table("CLEANED_INFLATION")
    
    # Return value will appear in the Results tab.
    return df_snowflake