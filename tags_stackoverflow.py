from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

import json
from pandas import json_normalize
from datetime import datetime

#define a python function to process the extracted data
def _process_data(ti):
    #creating a variable which is taking the data fetched by extract_data task from the api
    data = ti.xcom_pull(task_ids = "extract_data")
    data = data['items'][0]
    #processing the json data 
    processed_data = json_normalize({
        'id' : data['question_id'],
        'title' : data['title'],
        'tags' : ",".join(data["tags"]),
        'created_at' : datetime.fromtimestamp(data["creation_date"])
    })
    processed_data.to_csv('/tmp/processed_data.csv',sep ='|', index=None, header=False)

#define a python funtion to store the data in a postgres database table.
def _store_data():
    hook = PostgresHook(postgres_conn_id = 'postgres')
    hook.copy_expert(
        sql = "COPY overflowtags (id,title,tags,created_at) FROM stdin WITH DELIMITER as '|'",
        filename = '/tmp/processed_data.csv'
    )

#define the DAG workflow
with DAG ('overflow', start_date=datetime(2023,5,1), schedule_interval='@daily', catchup=False) as dag:
    #creating a table in the databse
    create_table = PostgresOperator(
        task_id = 'create_table',
        #create a connection for aiflow and postgres in airflow webserver
        postgres_conn_id = 'postgres',
        sql = '''
            CREATE TABLE IF NOT EXISTS overflowtags(
            id INTEGER PRIMARY KEY NOT NULL,
            title TEXT NOT NULL,
            tags TEXT NOT NULL,
            created_at TIMESTAMP
            );
            '''
    )
    #checking if the api endpoint is available
    is_api_available = HttpSensor(
        task_id = 'is_api_available',
        #create an http connection for api in airflow webserver
        http_conn_id = 'stack_api',
        endpoint = '2.3/questions',
        request_params= {'order': 'desc', 'sort': 'activity', 'site': 'stackoverflow'}
    )

    #fetch the data from the api
    extract_data = SimpleHttpOperator(
        task_id = 'extract_data',
        http_conn_id = 'stack_api',
        endpoint = '2.3/questions',
        method = 'GET',
        data = {'order': 'desc', 'sort': 'activity', 'site': 'stackoverflow'},
        response_filter = lambda response: json.loads(response.text),
        log_response = True
    )

    #processing the fetched data as required
    process_data = PythonOperator(
        task_id = 'process_data',
        #call the python function to process the data
        python_callable = _process_data
    )

    #store the resulting data in a table
    store_data = PythonOperator(
        task_id = 'store_data',
        python_callable = _store_data
    )

    #define the order of execution of the tasks
    create_table >> is_api_available >> extract_data >> process_data >> store_data