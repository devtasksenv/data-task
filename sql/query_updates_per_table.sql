# Distinct the measurement rows and list only table updates excluding duplications
SELECT DISTINCT ACCOUNT, FULL_TABLE_NAME, UPDATE_TIMESTAMP from TABLES_UPDATES
ORDER BY FULL_TABLE_NAME, MEASUREMENT_TIMESTAMP