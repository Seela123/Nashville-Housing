/*1.Window Functions - Ranking:
        Rank the properties based on their sale price, partitioned by the tax district, and display the top 3 most expensive properties in each district.
		*/

SELECT parcelID,landuse,saleprice,ownername,
RANK() OVER(partition by TaxDistrict order by saleprice DESC) as RANK_sals FROM 
houses

/*
     2.Common Table Expressions (CTEs) - Recursive:
        Use a recursive CTE to generate a hierarchical report of properties owned by individuals and companies, including nested ownership structures.
*/


WITH selling AS (
SELECT ParcelID, COUNT(ParcelID) AS PARCELE, landuse, propertyaddress, saledate, saleprice, soldasvacant
FROM Houses
GROUP BY ParcelID, landuse, propertyaddress, saledate, saleprice, soldasvacant
HAVING COUNT(ParcelID) > 1
UNION
SELECT ParcelID, COUNT(ParcelID) AS PARCELE, landuse, propertyaddress, saledate, saleprice, soldasvacant
FROM Houses
GROUP BY ParcelID, landuse, propertyaddress, saledate, saleprice, soldasvacant
HAVING COUNT(ParcelID) = 1)
SELECT * FROM selling


/*  3.Pivoting Data - Aggregation:
        Pivot the data to calculate the average sale price for properties sold as vacant and not vacant,
	categorized by the year they were built.
*/
SELECT YearBuilt,SalePrice , avg(saleprice) AS AVG_saleprice
FROM Houses
GROUP BY YearBuilt,SalePrice


SELECT ParcelID,LandUse,PropertyAddress,SaleDate,SalePrice,SoldAsVacant,YearBuilt
,CASE 
WHEN SalePrice > avg(saleprice)OVER (PARTITION BY yearbuilt ORDER BY soldasvacant DESC) THEN 'good_price'
ELSE 'bed_price'
end as total_prices
FROM Houses

SELECT YearBuilt,
CAST (AVG(CASE WHEN soldasvacant = 1 THEN SalePrice END) AS INT) AS soldprice_vacant,
CAST (AVG(CASE WHEN soldasvacant = 0 THEN SalePrice END) AS INT) AS soldprice_novacant
FROM Houses
GROUP BY yearbuilt

/*4.Advanced Joins - Subquery:
    Write a query to find properties that have been sold multiple times, 
    showing the change in sale price for each transaction compared to the previous sale.
		*/
		


SELECT 
NH1.UniqueID AS PropertyID,
NH1.propertyaddress AS PropertyAddress,
NH1.ownername AS OwnerName,
NH2.UniqueID AS ParentPropertyID
FROM 
Houses NH1
LEFT JOIN 
Houses NH2 ON NH1.ownername = NH2.OwnerName



	/*
    5.Error Handling - Stored Procedure:
        Develop a stored procedure to update property records, handling exceptions such as invalid parcel IDs or missing owner information.
*/

SELECT * FROM Houses

CREATE PROCEDURE upHouses
@uniqueid INT,
@parceleID NVARCHAR (50),
@landuse NVARCHAR (50),
@propertyaddress NVARCHAR (50),
@saledate DATE,
@SalePrice MONEY,
@legalreference NVARCHAR(50),
@soldasvacant BIT,
@owenername NVARCHAR(50),
@owneraddress NVARCHAR (50),
@acreage FLOAT ,
@taxdistrict NVARCHAR(50),
@landvalue INT,
@buildingvalue INT,
@totalvalue INT,
@yearbuilt SMALLINT,
@bedrooms TINYINT,
@fullbath TINYINT,
@halfbath TINYINT
AS
BEGIN 
UPDATE Houses
SET ParcelID = @parceleID,
LandUse = @landuse,
PropertyAddress = @propertyaddress,
SaleDate = @saledate,
SalePrice = @SalePrice,
LegalReference = @legalreference,
SoldAsVacant = @soldasvacant,
OwnerName = @owenername,
OwnerAddress = @owneraddress,
Acreage = @acreage,
TaxDistrict = @taxdistrict,
LandValue = @totalvalue,
BuildingValue = @buildingvalue,
TotalValue = @totalvalue,
YearBuilt = @yearbuilt,
Bedrooms = @bedrooms,
FullBath = @fullbath,
HalfBath = @halfbath
WHERE UniqueID = @uniqueid
END
 

EXEC upHouses 2045,43075,'SINGLE FAMILY2','1808 FOX CHASE DE, GOODLETSTVILLE','2013-04-09',240000.00,'20130412-0036475',1
,'FRAZIER, CYRENTHA LYNETTE',
'1808  FOX CHASE DR GOODLETTSVILLE TN',3.33,'GENERAL SERVICES DISTRICT',50001,168201,235701,1985,2,3,1

 SELECT * FROM Houses

/*6.Temporal Tables - Historical Analysis:
        Implement temporal tables to track changes in property values over time, and retrieve the historical values for a specific property based on its UniqueID.
*/

SELECT * FROM Houses

CREATE TABLE test_houses (
uniqueid INT PRIMARY KEY ,
parceleid int ,
landuse VARCHAR (50),
propertyaddress	NVARCHAR(MAX),
saleprice FLOAT,
legalreference FLOAT,
soldasvacant SMALLINT,
owername NVARCHAR (MAX),
oweraddress NVARCHAR (50),
acreage FLOAT,
taxdistrict NVARCHAR (50),
landvalue INT ,
buildingvalue INT,
totalvalue INT,
yearbuilt SMALLINT,
fullbath TINYINT,
halfbath TINYINT,
 SysStartTime DATETIME2 GENERATED ALWAYS AS ROW START NOT NULL
  ,SysEndTime DATETIME2 GENERATED ALWAYS AS ROW END NOT NULL
  ,PERIOD FOR SYSTEM_TIME (SysStartTime,SysEndTime)) WITH (SYSTEM_VERSIONING = ON (HISTORY_TABLE = dbo.test_housesHistory, DATA_CONSISTENCY_CHECK = ON));




INSERT INTO dbo.test_houses (uniqueid, parceleid, landuse, propertyaddress, saleprice, legalreference, soldasvacant, owername, oweraddress, acreage, taxdistrict, landvalue, buildingvalue, totalvalue, yearbuilt, fullbath, halfbath)
VALUES (3, 2001, 'Residential', '123 Main St', 250000, 12345, 0, 'selman bytyqi', '456 Elm St', 1.5, 'District 1', 100000, 150000, 250000, 1995, 2, 1);


UPDATE dbo.test_houses
SET saleprice = 260333
WHERE uniqueid = 1;

UPDATE test_houses
SET saleprice = 7000
WHERE uniqueid = 3;

SELECT * FROM test_houseshistory
SELECT * FROM test_houses

/* 7.Advanced Aggregations - Percentile Calculation:
        Calculate the 95th percentile of property values for each tax district, excluding outliers.

*/

SELECT PropertyAddress,SaleDate,SalePrice,
CASE 
WHEN MONTH(SaleDate) IN (12,1,2)  THEN 'WINTER'
WHEN MONTH(SaleDate) IN (3,4,5) THEN 'SPRING'
WHEN MONTH(SaleDate) IN (6,7,8) THEN'SUMER'
ELSE 'AUTUMN'
END AS 'SEASON'
FROM Houses

CREATE VIEW house_season AS (
SELECT PropertyAddress,SaleDate,SalePrice,
CASE 
WHEN MONTH(SaleDate) IN (12,1,2)  THEN 'WINTER'
WHEN MONTH(SaleDate) IN (3,4,5) THEN 'SPRING'
WHEN MONTH(SaleDate) IN (6,7,8) THEN'SUMER'
ELSE 'AUTUMN'
END AS 'SEASON'
FROM Houses
)

SELECT * FROM house_season

--calculate by month

SELECT DISTINCT(SEASON),SUM(saleprice) AS SaleSeason
FROM house_season
GROUP BY SEASON
ORDER BY SaleSeason DESC

 CREATE VIEW sale_percent AS
 (
SELECT DISTINCT(SEASON),SUM(saleprice) AS SaleSeason
FROM house_season
GROUP BY SEASON
)

SELECT * FROM sale_percent
ORDER BY SaleSeason DESC

SELECT *,SaleSeason/(SELECT SUM(saleseason) FROM sale_percent)*100 AS Percent_month
FROM sale_percent
ORDER BY Percent_month DESC

-- best sale prie by land use


SELECT DISTINCT(LandUse),AVG(SalePrice)OVER (PARTITION BY landuse  ) AS SalePriceTotal
FROM Houses
ORDER BY SalePriceTotal DESC


/*8.Dynamic SQL - Parameterized Query:
 Write a stored procedure that accepts input parameters such as land use and acreage range, 
 and dynamically generates a query to filter properties accordingly.
*/

CREATE PROCEDURE fastFineHouses 
@uniqueid INT
AS 
BEGIN
SELECT * FROM Houses
WHERE UniqueID = @uniqueid 
END

EXEC fastFineHouses 46503

















