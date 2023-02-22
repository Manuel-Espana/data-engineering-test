# Description: Flow that moves customer data and items to the Data Warehouse in BigQuery.
# Owner name: Jesús Manuel España Tzec.

from airflow import DAG
import datetime as dt
import pandas as pd
import pymongo
from pymongo import MongoClient
from google.cloud import bigquery
from airflow.models import XCom
from airflow.utils.db import provide_session
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

#######################################################################################
# Parameters
#######################################################################################
dag_name = 'dag_data_engineering_test'
project = '<PROYECT_NAME>'
owner = 'Jesús Manuel España Tzec'
GBQ_CONNECTION_ID = 'bigquery_default'


client_mongo = MongoClient("mongodb+srv://manuel_11:Manueles119@data-engineering-test.9tatrqe.mongodb.net/?retryWrites=true&w=majority")
#######################################################################################

# Description: function that invoke the SP that moves data from the tables Customer, Items, Items Bought to dwh.
# Parameters:
# * None.
def insert_to_dwh():
  client_bigquery = bigquery.Client()
  query_job = """DECLARE p_msg STRING DEFAULT NULL;
                 CALL `Pruebas.data_cleansing`(p_msg);
              """
  query_result = client_bigquery.query(query_job).result()
  print(query_result)


# Description: function that extract data from the data_engineering_test MongoDB database.
# Parameters:
# * collection: name of the collection where the data is stored .
def get_mongo_data(collection):
  db = client_mongo.data_engineering_test
  collection = db[collection]
  cursor = collection.find({})

  data = []
  for document in cursor:
        data.append(document)

  return data


# Description: function that apply some data wrangling to the customers data.
# Parameters:
# * None.
def wrangling_customers():
  df_customers = pd.DataFrame.from_dict(get_mongo_data('customers_data'))

  df_customers.firstname = df_customers.firstname.str.replace('\W', '')
  df_customers.lastname = df_customers.lastname.str.replace('\W', '')
  convert_col = {'_id': str}
  df_customers = df_customers.astype(convert_col)

  return df_customers


# Description: function that apply some data wrangling to the items data.
# Parameters:
# * None.
def wrangling_items():
  df_items = pd.DataFrame.from_dict(get_mongo_data('items_data'))

  df_items.title = df_items.title.str.replace('\W', '')
  convert_col = {'_id': str}
  df_items = df_items.astype(convert_col)

  return df_items


# Description: function that load the data from MongoDB to its Data Warehouse table in BigQuery.
# Parameters:
# * schema: BigQuery table schema.
# * df: dataframe where the data is.
# * table_id: id of the BigQuery table.
def sink_bigquery(schema, df, table_id):
  client_bigquery = bigquery.Client()
  job_config = bigquery.LoadJobConfig(schema = schema)
  job = client_bigquery.load_table_from_dataframe(df, table_id, job_config = job_config)


# Description: function that call the sink_bigquery function.
# Parameters:
# * None.
def insert_to_dwh_nosql():
  table_id_customers = 'Pruebas.dwh_customers_data_nosql'
  table_id_items = 'Pruebas.dwh_items_data_nosql'

  schema_items = [
      bigquery.SchemaField('_id', 'STRING', mode = 'REQUIRED'),
      bigquery.SchemaField('title', 'STRING', mode = 'NULLABLE'),
      bigquery.SchemaField('price', 'FLOAT', mode = 'NULLABLE')
  ]

  schema_customers = [
      bigquery.SchemaField('_id', 'STRING', mode = 'REQUIRED'),
      bigquery.SchemaField('firstname', 'STRING', mode = 'NULLABLE'),
      bigquery.SchemaField('lastname', 'STRING', mode = 'NULLABLE')
  ]
  sink_bigquery(schema_customers, wrangling_customers(), table_id_customers)
  sink_bigquery(schema_items, wrangling_items(), table_id_items)



default_args = {
    'owner': owner,
    'depends_on_past': False,
    'start_date': dt.datetime(2023, 2, 22),
    'retries': 0,
    'project_id': project,
}

with DAG(dag_name,
         default_args = default_args,
         catchup = False,
         #Scheduled to run every day at 12 am
         schedule_interval = '0 0 * * *',
         max_active_runs = 1) as dag:

    #############################################################
    
    t_begin = DummyOperator(task_id = 'begin')

    insert_to_dwh_task = PythonOperator(task_id = 'insert_to_dwh_task',
                            provide_context = True,
                            python_callable = insert_to_dwh)

    insert_to_dwh_nosql_task = PythonOperator(task_id = 'insert_to_dwh_nosql_task',
                            provide_context = True,
                            python_callable = insert_to_dwh_nosql)

    t_end = DummyOperator(task_id = 'end', trigger_rule = TriggerRule.NONE_FAILED)

    #############################################################
    t_begin >> insert_to_dwh_task >> insert_to_dwh_nosql_task  >> t_end