CREATE OR REPLACE TABLE DATA_ACADEMY.SNEAKER_FACTORY.INFLATION_INFO(
    COUNTRY_NAME varchar
    , COUNTRY_CODE varchar
    , INDICATOR_NAME varchar
    , INDICATOR_CODE varchar
    , "1960"  varchar
    , "1961"  varchar
    , "1962"  varchar
    , "1963"  varchar
    , "1964"  varchar
    , "1965"  varchar
    , "1966"  varchar
    , "1967"  varchar
    , "1968"  varchar
    , "1969"  varchar
    , "1970"  varchar
    , "1971"  varchar
    , "1972"  varchar
    , "1973"  varchar
    , "1974"  varchar
    , "1975"  varchar
    , "1976"  varchar
    , "1977"  varchar
    , "1978"  varchar
    , "1979"  varchar
    , "1980"  varchar
    , "1981"  varchar
    , "1982"  varchar
    , "1983"  varchar
    , "1984"  varchar
    , "1985"  varchar
    , "1986"  varchar
    , "1987"  varchar
    , "1988"  varchar
    , "1989"  varchar
    , "1990"  varchar
    , "1991"  varchar
    , "1992"  varchar
    , "1993"  varchar
    , "1994"  varchar
    , "1995"  varchar
    , "1996"  varchar
    , "1997"  varchar
    , "1998"  varchar
    , "1999"  varchar
    , "2000"  varchar
    , "2001"  varchar
    , "2002"  varchar
    , "2003"  varchar
    , "2004"  varchar
    , "2005"  varchar
    , "2006"  varchar
    , "2007"  varchar
    , "2008"  varchar
    , "2009"  varchar
    , "2010"  varchar
    , "2011"  varchar
    , "2012"  varchar
    , "2013"  varchar
    , "2014"  varchar
    , "2015"  varchar
    , "2016"  varchar
    , "2017"  varchar
    , "2018"  varchar
    , "2019"  varchar
    , "2020"  varchar
    , "2021"  varchar

)
;

COPY INTO DATA_ACADEMY.SNEAKER_FACTORY.INFLATION_INFO
FROM(
    SELECT 
    $1::varchar
    , $2::varchar
    , $3::varchar
    , $4::varchar
    , $5::varchar
    , $6::varchar
    , $7::varchar
    , $8::varchar
    , $9::varchar
    , $10::varchar
    , $11::varchar
    , $12::varchar
    , $13::varchar
    , $14::varchar
    , $15::varchar
    , $16::varchar
    , $17::varchar
    , $18::varchar
    , $19::varchar
    , $20::varchar
    , $21::varchar
    , $22::varchar
    , $23::varchar
    , $24::varchar
    , $25::varchar
    , $26::varchar
    , $27::varchar
    , $28::varchar
    , $29::varchar
    , $30::varchar
    , $31::varchar
    , $32::varchar
    , $33::varchar
    , $34::varchar
    , $35::varchar
    , $36::varchar
    , $37::varchar
    , $38::varchar
    , $39::varchar
    , $40::varchar
    , $41::varchar
    , $42::varchar
    , $43::varchar
    , $44::varchar
    , $45::varchar
    , $46::varchar
    , $47::varchar
    , $48::varchar
    , $49::varchar
    , $50::varchar
    , $51::varchar
    , $52::varchar
    , $53::varchar
    , $54::varchar
    , $55::varchar
    , $56::varchar
    , $57::varchar
    , $58::varchar
    , $59::varchar
    , $60::varchar
    , $61::varchar
    , $62::varchar
    , $63::varchar
    , $64::varchar
    , $65::varchar
    , $66::varchar
    FROM @AZUREBLOB/Inflation_Data
    (file_format => 'csv')
    
)
;