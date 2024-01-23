import sys 
# sys.path.insert(0, 'M:/airflow/dags/aftest')

from datetime import datetime, timedelta
# from data_getter import Data_getter as dg
# from service import Service as srv
# import pendulum 
from main import Main as m
import yaml
from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

PATH_TO_PYTHON_BINARY = sys.executable

@dag(
    dag_id = "some_test1",
    start_date = datetime.now(),
    schedule_interval = "5 * * * * ",
    catchup = False,
    tags = ["test"]
)
def some_test1():


    @task(task_id="execute")
    def execute():
        m().execute()


    execute()


dag=some_test1()
    # @task(task_id="get_config")
    # def get_config(path):
    #     with open(path) as c:
    #         config = yaml.safe_load(c)
    #     return config
    # run_this = get_config("M:/airflow/dags/aftest/config.yml")

    # @task(task_id="get_data_from_blizzard")
    # def get_data_blizz(config):
    #     data = dg(config).get_cards_in_json()
    #     return data
    
    # get_data = get_data_blizz(run_this)

    # @task(task_id="set_data_in_db")
    # def set_data_in_db(config, data):
    #     srv(config).set_data_in_database(data)
    #     return "scs"
    
    # set_data = set_data_in_db(get_data)

    # run_this >> get_data >> set_data
