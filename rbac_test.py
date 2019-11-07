"""
Code that goes along with the Airflow located at:
http://airflow.readthedocs.org/en/latest/tutorial.html
"""
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2019, 11, 6),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
    #"access_control": {"role1": {"can_dag_read", "can_dag_edit"}}
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG("rbac_test",
    default_args=default_args,
    schedule_interval=timedelta(minutes=1),
    catchup = False,
    access_control = {"role1": {"can_dag_read", "can_dag_edit"}}
    
)

# t1, t2 and t3 are examples of tasks created by instantiating operators
t1 = BashOperator(task_id="print_date", bash_command="date", dag=dag)

t2 = BashOperator(task_id="sleep", bash_command="sleep 5", retries=3, dag=dag)


t3 = BashOperator(task_id="sleep2", bash_command="sleep 5", retries=3, dag=dag)

t2.set_upstream(t1)
t3.set_upstream(t1)
