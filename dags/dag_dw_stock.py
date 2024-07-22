import pandas as pd
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine

from dags.sql.sql_stock import StockSQL
from dags.handles.readreplic_handle import ReadReplicHandle
from dags.handles.dw_handle import DWHandle

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.email_operator import EmailOperator
from airflow.utils.dates import days_ago

load_dotenv() 

# Credentials
readreplic_host = os.getenv('READREPLIC_HOST')
readreplic_port = os.getenv('READREPLIC_PORT')
readreplic_user = os.getenv('READREPLIC_USER')
readreplic_passwd = os.getenv('READREPLIC_PASSWORD')
readreplic_database = os.getenv('READREPLIC_DB')

dw_host = os.getenv('DW_HOST')
dw_port = os.getenv('DW_PORT')
dw_user = os.getenv('DW_USER')
dw_passwd = os.getenv('DW_PASSWORD')
dw_database = os.getenv('DW_DB')

# Connections
readreplic_handle = ReadReplicHandle(readreplic_host, 
                                        readreplic_port, 
                                        readreplic_user, 
                                        readreplic_passwd, 
                                        readreplic_database, 
                                        query=StockSQL.query(),
                                        fields=StockSQL.fields())
dw_handle = DWHandle(dw_host, 
                        dw_port, 
                        dw_user, 
                        dw_passwd, 
                        dw_database)
destination_table = 'stock'

# Funções
def query_execute():
    readreplic_handle.create_connection()
    data = readreplic_handle.query_data()

    # Add ID column
    data = data.reset_index() 
    data = data.rename(columns={"index": "id"})

    return data

def truncate_table():
    dw_handle.create_connection()
    dw_handle.query_data(query=f'TRUNCATE TABLE {destination_table}')

def insert_table():
    engine = create_engine(f"mysql+mysqldb://{dw_user}:{dw_passwd}@{dw_host}:{dw_port}/{dw_database}")
    con_engine = engine
    data = query_execute()
    print(data)
    data.to_sql(f'{destination_table}', con=con_engine, if_exists='append', index=False)

# DAG
with DAG(
    dag_id='dag_dw_stock', 
    description="ReadReplic to DW: Stock",
    schedule_interval= "0 0 * * *",
    start_date = days_ago(1),
    fail_stop=True,
    catchup=False
) as dag:

    begin = DummyOperator(
        task_id="begin",
        trigger_rule='all_success'
    )

    task_query_execute = PythonOperator(
        task_id="task_query_execute",
        python_callable=query_execute,
        provide_context=True,
        trigger_rule='all_success'
    )

    task_truncate_table = PythonOperator(
        task_id="task_truncate_table",
        python_callable=truncate_table,
        provide_context=True,
        trigger_rule='all_success'
    )

    task_insert_table = PythonOperator(
        task_id="task_insert_table",
        python_callable=insert_table,
        provide_context=True,
        trigger_rule='all_success'
    )

    email_on_failed = EmailOperator(
        task_id="email_failed_task",
        to='email_teste',
        subject=f"Falha na atualização da tabela de {destination_table}!",
        html_content=f"<i>A Atualização da tabela de {destination_table} falhou, confira o log no webserver para entender melhor o problema</i>",
        cc=['email_cc'],
        trigger_rule='one_failed'
    )
    
    end = DummyOperator(
        task_id="end",
        trigger_rule='all_success'
    )

begin >> task_query_execute >> task_truncate_table >> task_insert_table >> email_on_failed >> end
