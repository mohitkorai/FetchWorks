--To Create database fetchworks
create database fetchworks;

--To change the current database to fetchworks
use fetchworks;

--Given unstrucured JSON file with JSON objects is converted into JSON file using python to make the JSON file suitable for reading by SQL Server
--The Python code used to convert unstructed JSON file is also attached in this repo 
--Declaring new JSON Table
DECLARE @JSON_TABLE VARCHAR(MAX)
--With BulkColumn all the json objects are inserted into JSON Table
SELECT @JSON_TABLE = BulkColumn
FROM OPENROWSET(BULK '/tmp//users_f.json', SINGLE_CLOB)U
--Creating Table users
SELECT
users_id."$oid" as id,
--To convert UNIX Timestamp into datetime format
DATEADD(SECOND, users_createdDate."$date"/1000 ,'1970/1/1') as createdDate,
DATEADD(SECOND, users_lastLogin."$date"/1000 ,'1970/1/1') as lastLogin,
users_f.active,
users_f.role,
users_f.signUpSource,
users_f.state
INTO users
FROM OPENJSON(@JSON_TABLE)
WITH(
    _id NVARCHAR(MAX) as JSON,
    active CHAR(20),
    createdDate NVARCHAR(MAX) as JSON,
    lastLogin NVARCHAR(MAX) as JSON,
    role CHAR(20),
    signUpSource CHAR(20),
    state CHAR(20)
) as users_f
--To extract values from nested JSON object
CROSS APPLY OPENJSON(users_f._id)
WITH(
    "$oid" VARCHAR(30)
) as users_id
CROSS APPLY OPENJSON(users_f.createdDate)
WITH(
    "$date" BIGINT
) as users_createdDate
OUTER APPLY OPENJSON(users_f.lastLogin)
WITH(
    "$date" BIGINT
) as users_lastLogin;

SELECT TOP 100 * FROM users;


--Similar to users, brands table is created below
DECLARE @JSON_TABLE2 VARCHAR(MAX)

SELECT @JSON_TABLE2 = BulkColumn
FROM OPENROWSET(BULK '/tmp//brands_f.json', SINGLE_CLOB)B

SELECT 
brands_id."$oid" AS brand_id,
brands_f.barcode,
brands_f.brandCode,
brands_f.category,
brands_f.categoryCode,
brands_cpg."$ref" AS cpg_ref,
brands_cpgid."$oid" AS cpg_id,
brands_f.topBrand,
brands_f.name
INTO brands
FROM OPENJSON (@JSON_TABLE2)
WITH(
    _id NVARCHAR(MAX) AS JSON,
    barcode VARCHAR(30),
    brandCode VARCHAR(50),
    category VARCHAR(50),
    categoryCode VARCHAR(50),
    cpg NVARCHAR(MAX) AS JSON,
    name VARCHAR(50),
    topBrand CHAR(10)
) AS brands_f
CROSS APPLY OPENJSON(brands_f._id)
WITH(
    "$oid" VARCHAR(30)
) AS brands_id
CROSS APPLY OPENJSON(brands_f.cpg)
WITH(
    "$ref" CHAR(20),
    "$id" NVARCHAR(MAX) AS JSON
) AS brands_cpg
CROSS APPLY OPENJSON(brands_cpg."$id")
WITH(
    "$oid" VARCHAR(30)
) AS brands_cpgid;

SELECT TOP 100 * from brands;


--Similar to users, receipts table is created here
DECLARE @JSON_TABLE3 VARCHAR(MAX)

SELECT @JSON_TABLE3 = BulkColumn
FROM OPENROWSET(BULK '/tmp//receipts_f.json', SINGLE_CLOB)R

SELECT 
receipts_id."$oid" AS receipt_id,
receipts_f.bonusPointsEarned,
receipts_f.bonusPointsEarnedReason,
DATEADD(SECOND, receipts_createDate."$date"/1000 ,'1970/1/1') as createdDate,
DATEADD(SECOND, receipts_dateScanned."$date"/1000 ,'1970/1/1') as dateScanned,
DATEADD(SECOND, receipts_finishedDate."$date"/1000 ,'1970/1/1') as finishedDate,
DATEADD(SECOND, receipts_modifyDate."$date"/1000 ,'1970/1/1') as modifyDate,
DATEADD(SECOND, receipts_pointsAwardedDate."$date"/1000 ,'1970/1/1') as pointsAwardedDate,
receipts_f.pointsEarned,
DATEADD(SECOND, receipts_purchaseDate."$date"/1000 ,'1970/1/1') as purchaseDate,
receipts_f.purchasedItemCount,
receipts_f.rewardsReceiptItemList,
receipts_rewardsReceiptItemList.barcode,
receipts_rewardsReceiptItemList.brandCode,
receipts_rewardsReceiptItemList.finalPrice,
receipts_f.rewardsReceiptStatus,
receipts_f.totalSpent,
receipts_f.userId
INTO receipts
FROM OPENJSON(@JSON_TABLE3)
WITH(
    _id NVARCHAR(MAX) AS JSON,
    bonusPointsEarned INT,
    bonusPointsEarnedReason VARCHAR(MAX),
    createDate NVARCHAR(MAX) AS JSON,
    dateScanned NVARCHAR(MAX) AS JSON,
    finishedDate NVARCHAR(MAX) AS JSON,
    modifyDate NVARCHAR(MAX) AS JSON,
    pointsAwardedDate NVARCHAR(MAX) AS JSON,
    pointsEarned FLOAT,
    purchaseDate  NVARCHAR(MAX) AS JSON,
    purchasedItemCount INT,
    rewardsReceiptItemList NVARCHAR(MAX) AS JSON,
    rewardsReceiptStatus CHAR(20),
    totalSpent FLOAT,
    userId VARCHAR(30)
) AS receipts_f
--All the nested json objects are extratcted using the code below
CROSS APPLY OPENJSON(receipts_f._id)
WITH(
    "$oid" VARCHAR(30)
) AS receipts_id
OUTER APPLY OPENJSON(receipts_f.createDate)
WITH(
    "$date" BIGINT
) AS receipts_createDate
OUTER APPLY OPENJSON(receipts_f.dateScanned)
WITH(
    "$date" BIGINT
) AS receipts_dateScanned
OUTER APPLY OPENJSON(receipts_f.finishedDate)
WITH(
    "$date" BIGINT
) AS receipts_finishedDate
OUTER APPLY OPENJSON(receipts_f.modifyDate)
WITH(
    "$date" BIGINT
) AS receipts_modifyDate
OUTER APPLY OPENJSON(receipts_f.pointsAwardedDate)
WITH(
    "$date" BIGINT
) AS receipts_pointsAwardedDate
OUTER APPLY OPENJSON(receipts_f.purchaseDate)
WITH(
    "$date" BIGINT
) AS receipts_purchaseDate
--Only few features present in nested rewardsReceiptItemList object are extracted
CROSS APPLY OPENJSON(receipts_f.rewardsReceiptItemList)
WITH(
    barcode VARCHAR(30),
    brandCode VARCHAR(50),
    finalPrice FLOAT
) AS receipts_rewardsReceiptItemList; 

SELECT TOP 100 * FROM receipts;


-- The most recent receipt was scanned on 2021 March 1st, therefore 2021 February is taken as most recent month for receipt scanned
SELECT MAX(dateScanned) FROM receipts;


--Q1
SELECT r.brandCode,b.name, Count(Distinct receipt_id) AS receipts_scanned
FROM receipts r 
LEFT JOIN brands b ON r.brandCode=b.brandCode
WHERE CAST(dateScanned AS DATE) >= '2021-02-01' AND CAST(dateScanned AS DATE)<='2021-02-28' AND r.brandCode IS NOT NULL
Group By r.brandCode,b.name
Order BY Count(Distinct receipt_id) DESC;
--The most recent receipt was scanned on 2021 March 1st, therefore 2021 February is taken as most recent month for receipt scanned
--Only 3 different brands receipts were scanned with BRAND 3 receipts, MISSION 2 receipts and VIVA 1 receipt were scanned


--Q2
SELECT TOP 5 r.brandCode, b.name, Count(DISTINCT receipt_id) AS receipts_scanned
FROM receipts r
LEFT JOIN brands b ON r.brandCode = b.brandCode
WHERE CAST(dateScanned AS DATE) >= '2021-01-01' AND CAST(dateScanned AS DATE)<='2021-01-31' AND r.brandCode IS NOT NULL
Group By r.brandCode,b.name
Order BY Count(DISTINCT receipt_id) DESC;
--The Top 5 Brands for previous month January 2021 are BEN AND JERRYS, FOLGERS, PEPSI, KELLOGG'S, KRAFT


--Q3
SELECT rewardsReceiptStatus, AVG(totalSpent) AS Average_TotalSpent
FROM receipts
WHERE rewardsReceiptStatus='FINISHED' OR rewardsReceiptStatus='REJECTED'
GROUP BY rewardsReceiptStatus
ORDER BY AVG(totalSpent) DESC;
--Average Total Spend is greater for receipts where rewardsReceiptStatus is FINISHED


--Q4
SELECT rewardsReceiptStatus, SUM(purchasedItemCount) AS Total_No_of_ItemsPurchased
FROM receipts
WHERE rewardsReceiptStatus='FINISHED' OR rewardsReceiptStatus='REJECTED'
GROUP BY rewardsReceiptStatus
ORDER BY SUM(purchasedItemCount) DESC;
--Total number of items purchased is greater for receipts where rewardsReceiptStatus is FINISHED


--Q5
SELECT TOP 1 r.brandCode,r.finalPrice
FROM receipts r
LEFT JOIN users u ON r.userId = u.id
WHERE  CAST(u.createdDate AS DATE) >= DATEADD(day,-180,'2021-03-01') AND CAST(u.createdDate AS DATE) <'2021-03-01' AND r.brandCode IS NOT NULL
ORDER BY r.finalPrice DESC;
-- HEMPLER's Brand has most spend of 223.36$ for users created in the past 6 months assuming today's date to be '2021-03-1'


--Q6
SELECT TOP 1 r.brandCode,COUNT(DISTINCT r.receipt_id) AS NO_OF_Transactions
FROM receipts r
LEFT JOIN users u ON r.userId = u.id
WHERE  CAST(u.createdDate AS DATE) >= DATEADD(day,-180,'2021-03-01') AND CAST(u.createdDate AS DATE) <'2021-03-01' AND r.brandCode IS NOT NULL
GROUP BY r.brandCode
ORDER BY COUNT(DISTINCT r.receipt_id) DESC;
--BRAND has highest no of transactions which is 20 transactions for users created in the past 6 months assuming today's date to be '2021-03-1'
