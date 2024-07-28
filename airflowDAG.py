from airflow import DAG
from airflow.sensors.filesystem import FileSensor 
from airflow.operators.bash import BashOperator
from airflow.operators.python  import PythonOperator
from datetime import datetime, timedelta
import sys
sys.path.insert(0,"/home/mcmac/airflow/dags")
from GoogleAPI_to_RDS import main as loadDataMethod
from readDataFromDB import main as readDataMethod

def py_loadData(**kwargs):
    print('Loader started @ ',kwargs['params']['process_name'])
    loadDataMethod()

def py_readData(**kwargs):
    print('Reader started @ ',kwargs['params']['process_name'])
    readDataMethod()

default_args = {
    'owner': 'kmth-hash',
    'depends_on_past': False,
    'start_date': datetime(2023, 11, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG('AirflowRunner', default_args=default_args, schedule='@once') as dag : 
    p1 = PythonOperator(
        task_id='Pythonloader', 
        python_callable = py_loadData,
        params={'startTime':datetime.now() , 'process_name':'Pythonloader'},
        dag=dag
    ) 

    p2 = PythonOperator(
        task_id='Pythonreader', 
        python_callable = py_readData,
        params={'startTime':datetime.now() , 'process_name':'Pythonreader'},
        dag=dag
    ) 

    p3 = BashOperator(
        task_id='ClearDB' , 
        bash_command='mysql -u username -ppassword zocket < /home/mcmac/airflow/dags/testsql.sql',
        output_encoding='utf-8',
        dag=dag 
    )

    p3 >> p1 >> p2 