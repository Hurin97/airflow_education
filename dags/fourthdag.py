import pendulum
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

@dag(
    dag_id = "recieving_buyers_by_country_v2",
    start_date = pendulum.datetime(2024, 1, 1),
    catchup = False,
    schedule_interval = "0 */1 * * *",
    tags = ["test"]
)
def receive_buyers():
    
    def sql_operation(task_id, sql):
        return PostgresOperator(task_id = task_id, postgres_conn_id = "pg_db_shop", sql = sql,)
    
    sql_for_recieve_data = "select  country, count(email) as numbers from test.shop group by country order by country asc"
    
    sql_temp_t = "Create table if not exists test.buyers_by_country_temp(country text,numbers text)"
    
    def sql_insert(values):
        return f"Insert into test.buyers_by_country_temp(country, numbers)values{values}"

    sql_merge = "Drop table if exists test.buyers_by_country; Alter table test.buyers_by_country_temp rename to buyers_by_country;"
        
    @task(task_id="receiving_data")
    def receiving_data(sql):
        postgres_hook = PostgresHook(postgres_conn_id="pg_db_shop")
        conn = postgres_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(sql)
        res = ''
        for line in cursor:
            res+= str(line) +','
        return res.rstrip(',')
    
    recieve_buyers_by_country = receiving_data(sql_for_recieve_data)

    create_necessary_temp_table = sql_operation("create_temp_table",sql_temp_t)

    insert_data = sql_operation("insert_data_to_temp_table", sql_insert(recieve_buyers_by_country))

    merge_data = sql_operation("merdge_data", sql_merge)

    [recieve_buyers_by_country, create_necessary_temp_table] >> insert_data >> merge_data

dag = receive_buyers()   