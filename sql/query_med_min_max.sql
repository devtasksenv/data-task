# Use Lag function to get previous row for date comparison 
WITH TABLES_UPDATES_CTE
AS (SELECT *, Lag(UPDATE_TIMESTAMP) OVER(
       ORDER BY FULL_TABLE_NAME, MEASUREMENT_TIMESTAMP) 
       AS PREVIOUS_UPDATE,
          Lag(FULL_TABLE_NAME) OVER(
       ORDER BY FULL_TABLE_NAME, MEASUREMENT_TIMESTAMP) 
       AS PREVIOUS_FULL_TABLE_NAME FROM TABLES_UPDATES
       ORDER BY FULL_TABLE_NAME, MEASUREMENT_TIMESTAMP),
# Create CTE data from table with row numbers for values (med, min, max) calucation
TABLES_UPDATES_CTE_ORDERED AS
(SELECT distinct ACCOUNT, FULL_TABLE_NAME,
	   row_number() OVER (PARTITION BY FULL_TABLE_NAME 
       ORDER BY TIMESTAMPDIFF(SECOND, PREVIOUS_UPDATE, UPDATE_TIMESTAMP) DESC) AS R_NUMBER_DESC,
	   TIMESTAMPDIFF(SECOND, PREVIOUS_UPDATE, UPDATE_TIMESTAMP) AS CN_UPDATE_DIFF
	   FROM TABLES_UPDATES_CTE
       WHERE FULL_TABLE_NAME = PREVIOUS_FULL_TABLE_NAME AND PREVIOUS_UPDATE != UPDATE_TIMESTAMP),
TABLES_UPDATES_CTE_ORDERED_DESC AS
(SELECT *, row_number() OVER (PARTITION BY FULL_TABLE_NAME 
       ORDER BY R_NUMBER_DESC DESC) AS R_NUMBER from TABLES_UPDATES_CTE_ORDERED),
# Calculate median sum for both even and odd states
MEDIAN_SUM
AS (SELECT ACCOUNT, FULL_TABLE_NAME,
row_number() OVER (PARTITION BY FULL_TABLE_NAME ORDER BY CN_UPDATE_DIFF) AS R_NUMBER_MED,
sum(CN_UPDATE_DIFF) OVER (PARTITION BY FULL_TABLE_NAME ORDER BY CN_UPDATE_DIFF) AS MEDIAN_DIFF_SUM
FROM TABLES_UPDATES_CTE_ORDERED_DESC 
WHERE R_NUMBER BETWEEN R_NUMBER_DESC - 1 AND R_NUMBER_DESC + 1),
# Calculate median value
MEDIAN_RESULT 
AS (SELECT ACCOUNT, FULL_TABLE_NAME,
CASE WHEN R_NUMBER_MED = 1 THEN MEDIAN_DIFF_SUM
ELSE MEDIAN_DIFF_SUM/2
END AS MED_TBU
FROM MEDIAN_SUM MEDIAN_SUM_A
WHERE R_NUMBER_MED = (
    SELECT MAX(R_NUMBER_MED)
    FROM MEDIAN_SUM MEDIAN_SUM_B
    WHERE MEDIAN_SUM_A.FULL_TABLE_NAME = MEDIAN_SUM_B.FULL_TABLE_NAME
)),
# Calculate min value
MIN_RESULT 
AS (SELECT ACCOUNT, FULL_TABLE_NAME, CN_UPDATE_DIFF AS MIN_TBU
FROM TABLES_UPDATES_CTE_ORDERED_DESC WHERE R_NUMBER=1),
# Calculate max value
MAX_RESULT
AS (SELECT ACCOUNT, FULL_TABLE_NAME, CN_UPDATE_DIFF as MAX_TBU
FROM TABLES_UPDATES_CTE_ORDERED_DESC WHERE R_NUMBER_DESC=1),
# LATEST_UPDATE
LATEST_UPDATES
AS (SELECT distinct ACCOUNT, FULL_TABLE_NAME, max(UPDATE_TIMESTAMP) AS LATEST_UPDATE
FROM tables_updates GROUP BY ACCOUNT, FULL_TABLE_NAME),
# TBU values
TBU_VALUES
AS (SELECT MEDIAN_RESULT.ACCOUNT AS ACCOUNT, 
MEDIAN_RESULT.FULL_TABLE_NAME AS FULL_TABLE_NAME, 
LATEST_UPDATES.LATEST_UPDATE AS LATEST_UPDATE,
MEDIAN_RESULT.MED_TBU AS MED_TBU, 
MIN_RESULT.MIN_TBU AS MIN_TBU,
MAX_RESULT.MAX_TBU AS MAX_TBU
FROM MAX_RESULT INNER JOIN MIN_RESULT INNER JOIN MEDIAN_RESULT INNER JOIN LATEST_UPDATES
WHERE MEDIAN_RESULT.FULL_TABLE_NAME = MIN_RESULT.FULL_TABLE_NAME 
AND MEDIAN_RESULT.FULL_TABLE_NAME = MAX_RESULT.FULL_TABLE_NAME AND MEDIAN_RESULT.FULL_TABLE_NAME = LATEST_UPDATES.FULL_TABLE_NAME),
# TBU values and latest updates
TBU_VALUES_LATEST_UPDATES 
AS (SELECT * from TBU_VALUES
UNION
SELECT ACCOUNT, FULL_TABLE_NAME, LATEST_UPDATE, null AS MED_TBU, null AS MIN_TBU, null AS MAX_TBU
FROM LATEST_UPDATES WHERE FULL_TABLE_NAME NOT IN (SELECT FULL_TABLE_NAME FROM TBU_VALUES))
# Create table contains TBU values and latest updates
SELECT ACCOUNT, FULL_TABLE_NAME, MED_TBU, MAX_TBU, MIN_TBU FROM TBU_VALUES_LATEST_UPDATES;