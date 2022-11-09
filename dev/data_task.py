import csv
import IPython
import mysql.connector
import statistics
from datetime import datetime
import consts
from env import SQL_ENV

# Connect to SQL Database
connection = mysql.connector.connect(**SQL_ENV)
connection.autocommit = True
cursor = connection.cursor(buffered=True)
columns = None
DATE_IDX = 0
ACCOUNT_IDX = 0
TABLE_COLUMN_IDX = 1
UPDATE_COLUMN_IDX = 3
insert_sample = ["account1","db:schema_lineage.looker_view_nodes",
                            "2022-06-13 10:16:25.000","2022-06-12 09:08:19.000"]

def insert_csv_data():
  with open ('./data/data_set.csv', 'r') as data_csv_file:
      reader = csv.reader(data_csv_file)
      
      # Read lines from CSV file
      next(reader)
      global columns 
      columns = next(reader) 

      # Insert data to SQL Server
      for data in reader:
        query = consts.DATA_INSERTION_QUERY.format(','.join(columns), str(data).strip('[]'))
        cursor.execute(query)

def create_updates_table():
  cursor.execute(consts.CREATE_TABLE_QUERY)
  
def query_med_min_max():
  with open('./sql/query_med_min_max.sql') as sql_file:
    cursor.execute(sql_file.read())
    print(cursor.fetchall())
    
def query_updates_per_table():
  with open('./sql/query_updates_per_table.sql') as sql_file:
    cursor.execute(sql_file.read())
    print(cursor.fetchall())

# Create tables and indexes for optimization
def create_tables_and_indexes():
  with open('./sql/create_table_tbu_values.sql') as sql_file:
    cursor.execute(sql_file.read())
    print(cursor.fetchall())
  with open('./sql/create_index_tbu_values.sql') as sql_file:
    cursor.execute(sql_file.read())
    print(cursor.fetchall())
  with open('./sql/create_table_updates.sql') as sql_file:
    cursor.execute(sql_file.read())
    print(cursor.fetchall())
  with open('./sql/create_index_updates.sql') as sql_file:
    cursor.execute(sql_file.read())
    print(cursor.fetchall())

def insertion_trigger(account, table_name, update_date):
  cursor.execute(consts.CHECK_NEW_UPDATE_QUERY.format("'" + table_name + "'"))
  prev_update = cursor.fetchone()[DATE_IDX]
  current_update = datetime.strptime(update_date, '%Y-%m-%d %H:%M:%S.%f')
  
  # Check for new update
  if current_update > prev_update:
    
    # On new update found, calculate date-diff and insert
    date_diff = current_update - prev_update
    date_diff_secs = date_diff.seconds
    columns = ['ACCOUNT', 'FULL_TABLE_NAME', 'PREVIOUS_UPDATE', 'UPDATE_TIMESTAMP', 'CN_UPDATE_DIFF']
    values = [account, table_name, str(prev_update), str(current_update), date_diff_secs]
    query = consts.INSERT_LATEST_UPDATE_QUERY.format(','.join(columns), str(values).strip('[]'))
    cursor.execute(query)
    
    # Calculate new median, min and max values for the table updates
    cursor.execute(consts.GET_DATE_DIFFS_QUERY.format("'" + table_name + "'"))
    diff_values = [item[0] for item in cursor.fetchall()]
    columns = "LATEST_UPDATE = '{}', MED_TBU = {}, MIN_TBU = {}, MAX_TBU  = {}"
    values = columns.format(
                str(current_update), statistics.median(diff_values), 
                                  min(diff_values), max(diff_values))
    query = consts.INSERT_LATEST_TBU_QUERY + values + " WHERE FULL_TABLE_NAME = " + "'" + table_name + "'"

def data_insertion():
  columns = ['ACCOUNT', 'FULL_TABLE_NAME', 'MEASUREMENT_TIMESTAMP', 'UPDATE_TIMESTAMP']
  cursor.execute(consts.DATA_INSERTION_QUERY.format(','.join(
                columns), str(insert_sample).strip('[]')))
  insertion_trigger(
    insert_sample[ACCOUNT_IDX], insert_sample[TABLE_COLUMN_IDX], insert_sample[UPDATE_COLUMN_IDX])
  
    
def data_task():
  create_updates_table()
  insert_csv_data()
  query_med_min_max()
  query_updates_per_table()
  create_tables_and_indexes
  data_insertion()
  connection.close()

data_task()