-- Question 2.1

SELECT StoreID
		, SUM(NetPrice) 	
FROM FACT_ORDERLINES 
WHERE YEAR(OrderDate) = 2022  
GROUP BY StoreID
ORDER BY SUM(NetPrice) desc
;


-- Question 2.2
-- With CTE??
WITH CTE1 AS
(SELECT EmployeeID
		, SUM(NetPrice) AS TotalSales
FROM FACT_ORDERLINES
WHERE YEAR(OrderDate) = 2022
GROUP BY EmployeeID
),
CTE2 AS(
SELECT EmployeeID
		, SUM(ReturnAmount) AS TotalReturn
FROM FACT_RETURNS
WHERE YEAR(ReturnDate) = 2022
GROUP BY EmployeeID
)
SELECT CTE1.EmployeeID
        , CTE1.TotalSales
        , CTE2.TotalReturn
FROM CTE1
LEFT JOIN CTE2 ON CTE1.EmployeeID = CTE2.EmployeeID
ORDER BY EMPLOYEEID asc
;


-- Question 2.3

WITH CTE1 AS
(
SELECT StoreID
		, YEAR(OrderDate)::varchar AS SalesYear
        , SUM(NetPrice)::number(38,2) AS TotalSalesCurrentYear
        
FROM FACT_ORDERLINES
WHERE YEAR(ORDERDATE) >= 2019
GROUP BY StoreID, YEAR(OrderDate)
ORDER BY STOREID, YEAR(ORDERDATE)
),
CTE2 AS
(
SELECT StoreID
		, SalesYear
        , TotalSalesCurrentYear
        , LAG(TotalSalesCurrentYear) OVER(PARTITION BY StoreID ORDER BY SalesYear)::number(38,2) AS TotalSalesPrevYear
        
FROM CTE1
)
SELECT *
    , (((TotalSalesCurrentYear - TotalSalesPrevYear)/ TotalSalesPrevYear) * 100)::number(38,2) AS SalesRatio
FROM CTE2
ORDER BY StoreID, SalesYear
;


-- Question 2.4

select *
from fact_returns
where year(returndate) is null


;
WITH CTE1 AS
(
SELECT StoreID
		, YEAR(ReceivedDate)::varchar AS ReturnsYear
        , SUM(ReturnAmount)::number(38,2) AS TotalReturnsCurrentYear
FROM FACT_RETURNS
GROUP BY StoreID, YEAR(ReceivedDate)
ORDER BY StoreID, YEAR(ReceivedDate)
),
CTE2 AS
(
SELECT StoreID
		, ReturnsYear
        , TotalReturnsCurrentYear
        , LAG(TotalReturnsCurrentYear) OVER(PARTITION BY StoreID ORDER BY ReturnsYear)::number(38,2) AS TotalReturnsPrevYear
        
FROM CTE1
)
SELECT *
    , (((TotalReturnsCurrentYear - TotalReturnsPrevYear)/ TotalReturnsPrevYear) * 100)::number(38,2) AS ReturnsRatio
FROM CTE2
ORDER BY StoreID, ReturnsYear
;


-- Question 2.5
WITH EMPSAL AS
(
SELECT EmployeeID
		, SUM(NetPrice) AS TotalSales
        , YEAR(OrderDate) AS SalesYear
FROM FACT_ORDERLINES
WHERE YEAR(OrderDate) = 2022
GROUP BY EmployeeID, YEAR(OrderDate)
),

EMPRET AS
(
SELECT EmployeeID
		, SUM(ReturnAmount) AS TotalReturn
        , YEAR(ReceivedDate) AS ReturnYear
FROM FACT_RETURNS
WHERE YEAR(ReceivedDate) = 2022
GROUP BY EmployeeID, YEAR(ReceivedDate)
),

CTEQ22 AS 
(
SELECT EMPSAL.*
        , EMPRET.TotalReturn
FROM EMPSAL
LEFT JOIN EMPRET ON EMPSAL.EmployeeID = EMPRET.EmployeeID
)

SELECT CTEQ22.EmployeeID
        , EMP.FirstName
        , EMP.LastName
        , CTEQ22.SalesYear AS Year
        , CTEQ22.TotalSales
		, CTEQ22.TotalReturn
        , EMP.City
        , EMP.Country
FROM CTEQ22
LEFT JOIN DIM_EMPLOYEE EMP ON CTEQ22.EmployeeID = EMP.EmployeeID
WHERE CTEQ22.EmployeeID = 'SP10075'
;


-- Question 2.6

WITH CTE1 AS
(
SELECT   StoreID
		, ProductID
        , SUM(Quantity) AS QuantitySold
FROM FACT_ORDERLINES
GROUP BY StoreID, ProductID
ORDER BY StoreID asc, SUM(Quantity) desc
),

CTE2 AS
(
SELECT row_number() OVER(PARTITION BY STOREID ORDER BY QuantitySold desc) AS Rank_Product
        , CTE1.*
FROM CTE1
)

SELECT CTE2.*
		, PROD.ProductName
        , PROD.ProductCategory
        , PROD.ProductColor
FROM CTE2
LEFT JOIN DIM_PRODUCT PROD ON CTE2.ProductID = PROD.ProductID
WHERE Rank_Product IN (1,2,3,4,5)
ORDER BY StoreID asc, Rank_Product asc

