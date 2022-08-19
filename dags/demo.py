from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
from random import randint
import pandas as pd
import os
import json
#dag_path=os.getcwd()
AIRFLOW_HOME = os.getenv('AIRFLOW_HOME')


def adding_date_column():
    df1 = pd.DataFrame(
        {'a': range(0, 10), 'b': range(50, 60), 'c': range(100, 110)})
    #df1=pd.read_csv(f"{AIRFLOW_HOME}/Before/SampleCSVFile.csv",encoding='latin1')
    #df1.columns=[f"Column{x}" for x in range(3)]
    df1.insert(3, "d", [datetime.now().time()
               for x in range(len(df1["a"]))], True)
    #df1.to_csv(f"{AIRFLOW_HOME}/After/SampleCSVFile.csv")
    return df1.to_json()


with DAG("excel_transform",
         start_date=datetime(2022, 1, 1),
         schedule_interval='* * * * *',
         catchup=False) as dag:
	training_model_tasks = PythonOperator(
    	task_id="excel_transform",
    	python_callable=adding_date_column)

adding_date_column()