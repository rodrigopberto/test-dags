import logging
import json
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime

from airflow.operators.python_operator import PythonOperator

import logging
import docker

log = logging.getLogger(__name__)


def do_test_docker():
    for image in docker.from_env().images.list():
        log.info(image)
        
def launch_docker_container(**context):
    cli = docker.from_env()
    
    image_name = context['image_name']
   
    cli.images.pull(image_name)

    log.info(f"Creating image {image_name}")
    # get environment variables from UI
    try:
        environment = Variable.get(image_name, deserialize_json=True)
    except:
        environment = dict()

    environment['EXECUTION_ID'] = (context['dag_run'].run_id)
    
    container: Container = cli.containers.run(detach=True, image=image_name, environment=environment)
    container_id = container.id
    log.info(f"Running container with id {container_id}")

    logs = container.logs(follow=True, stderr=True, stdout=True, stream=True, tail='all')

    try:
        while True:
            l = next(logs)
            log.info(f"Task log: {l}")
    except StopIteration:
        log.info("Docker has finished!")
    
    inspect = cli.api.inspect_container(container_id)
    log.info(inspect)
    log.info(inspect)
    if inspect['State']['ExitCode'] != 0:
                raise Exception("Container has not finished with exit code 0")
    result = json.loads('["foo", {"bar":["baz", null, 1.0, 2]}]')
    
    log.info(f"Result was {result}")
    context['task_instance'].xcom_push('result', result, context['execution_date'])



default_args = {
    'owner': 'airflow',
    'start_date': datetime(2019, 11, 3)
}

def read_xcoms(**context):
    for idx, task_id in enumerate(context['data_to_read']):
        data = context['task_instance'].xcom_pull(task_ids=task_id, key='data')
        logging.info(f'[{idx}] I have received data: {data} from task {task_id}')


with DAG('energy_update_test', default_args=default_args, catchup=False) as dag:
    t1 = BashOperator(
        task_id='print_date1',
        bash_command='date')

    t2_1_id = 'do_task_one'
    t2_1 = PythonOperator(
        task_id=t2_1_id,
        provide_context=True,
        op_kwargs={
            'image_name': 'rodrigocver/up_energy:latest',
            'my_id': t2_1_id
        },
        python_callable=launch_docker_container
    )

    

    t3 = PythonOperator(
        task_id='read_xcoms',
        provide_context=True,
        python_callable=read_xcoms,
        op_kwargs={
            'data_to_read': [t2_1_id]
        }
    )

    t1 >> t2_1 >> t3
