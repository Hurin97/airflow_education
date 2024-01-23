from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
import pendulum

@dag(
    dag_id = 'some_test2',
    schedule_interval = '5 * * * * ',
    start_date = pendulum.now(),
    
    # catchup = False,
    tags = ['test2']
)
def some_test2():
    
    @task(task_id="follow_him")
    def follow_h():
        print("Follow him or her")


    follow_h()


dag = some_test2()