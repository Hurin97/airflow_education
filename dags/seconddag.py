import sys 
import yaml
from data_getter import Data_getter
from service import Service 
from datetime import datetime, timedelta
PATH_TO_PYTHON_BINARY = sys.executable
import yaml
from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

@dag(
    dag_id = "some_test3",
    start_date = datetime.now(),
    schedule_interval = "5 * * * * ",
    catchup = False,
    tags = ["test"]
)
def some_test3():

    
    def get_config():
        config='None'
        try:
            with open('/opt/airflow/dags/config.yml') as c:
                config = yaml.safe_load(c)
        except OSError as err:
            print("OS error:{0}".format(err))
        return config
    
    config=get_config()
    
    @task(task_id="receiving_data")
    def receive(config):
        return Data_getter(config).get_cards_in_json()
        
    data = receive(config)

    @task(task_id="load_data")
    def load_cards(cards, config):
        if cards!= "Empty response":
            Service(config).set_data_in_database(cards)

    load = load_cards(data, config)

    data >> load

dag = some_test3()