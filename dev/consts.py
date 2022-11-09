CREATE_TABLE_QUERY = """CREATE TABLE TABLES_UPDATES (
    ACCOUNT varchar(255),
    FULL_TABLE_NAME varchar(255),
    MEASUREMENT_TIMESTAMP DATETIME(3),
    UPDATE_TIMESTAMP DATETIME(3))"""
DATA_INSERTION_QUERY = """INSERT INTO TABLES_UPDATES({0}) VALUES ({1})"""
CHECK_NEW_UPDATE_QUERY = """SELECT LATEST_UPDATE FROM TBU_LATEST_UPDATES
                        WHERE FULL_TABLE_NAME={0}"""
GET_DATE_DIFFS_QUERY = """SELECT CN_UPDATE_DIFF FROM TABLE_LATEST_UPDATES
                        WHERE FULL_TABLE_NAME={0}"""
INSERT_LATEST_TBU_QUERY = """UPDATE
  TBU_LATEST_UPDATES SET """
INSERT_LATEST_UPDATE_QUERY = """INSERT INTO
  TABLE_LATEST_UPDATES({0}) VALUES ({1})"""
