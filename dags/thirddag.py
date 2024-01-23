import pendulum

from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

@dag(
    dag_id = "recieving_buyers_by_country",
    start_date = pendulum.datetime(2024, 1, 1),
    catchup = False,
    schedule_interval = "0 */1 * * *",
    tags = ["test"]
)
def receive_buyers():
    create_table = "Create table IF not EXISTS test.buyers_by_country(country text, numbers text)"

    create_temp_table = "DROP TABLE IF EXISTS test.buyers_by_country_temp; Create table test.buyers_by_country_temp(country text,numbers text)"

    merge_sql = """Insert into test.buyers_by_country
        Select *
        from (Select distinct *
        from test.buyers_by_country_temp) t
        on conflict ('country') do update
        Set
        'number'=excluded.'country'"""
    
    recieve_sql = """
             select  country, count(email) as numbers 
             from test.shop group by country order by country asc ;
         """
    
    def sql_operation(task_id, sql):
        return PostgresOperator(task_id = task_id, postgres_conn_id = "pg_db_shop", sql = sql,)
    
    def sql_insert(values):
        return f"Insert into test.buyers_by_country_temp(country, numbers)values{values}"
    
    @task(task_id="receiving_data")
    def receiving_data(sql):
        postgres_hook = PostgresHook(postgres_conn_id="pg_db_shop")
        conn = postgres_hook.get_conn()
        cursor = conn.cursor()
        data = cursor.execute(sql)
        res = ''
        for line in cursor:
            res+= str(line) +','
        return res.rstrip(',')
    
    recieve_buyers_by_country = receiving_data(recieve_sql)

    

    create_necessary_table = sql_operation("table_creating", create_table)

    create_necessary_temp_table = sql_operation("temp_table_creating", create_temp_table)

    insert_data = sql_operation("insert_data", sql_insert(recieve_buyers_by_country))
    
    @task(task_id = "merge")
    def merge_data(sql):
        try: 
            postgres_hook = PostgresHook(postgres_conn_id = "pg_db_shop")
            conn = postgres_hook.conn()
            cursor = conn.cursor()
            cursor.execute(sql)
            conn.commit()
            return 0
        except Exception as e:
            return 1
        
    merge = merge_data(merge_sql)
    
    [recieve_buyers_by_country, create_necessary_table, create_necessary_temp_table] >> insert_data >> merge

dag = receive_buyers()