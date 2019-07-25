from __future__ import print_function

import time
from builtins import range
from pprint import pprint

import airflow
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

args = {
    'owner': 'Muriilo - Teste Creditas',
    'start_date': airflow.utils.dates.days_ago(2),
    'description': '''Pipeline execution process, read datas of the marketing 
    (google, face, pageview and customer funnel).''',
}

dag = DAG(
    dag_id='example_python_operator',
    default_args=args,
    schedule_interval=None,
)

DAG_NAME = 'MarketingPipeline'


start_dummy = DummyOperator(
    task_id='Inicio',
    dag=dag)


def print_context(ds, **kwargs):
    pprint(kwargs)
    print(ds)
    return 'Whatever you return gets printed in the logs'


run_this = PythonOperator(
    task_id='print_the_context',
    provide_context=True,
    python_callable=print_context,
    dag=dag,
)

end_dummy = DummyOperator(
    task_id='Fim',
    dag=dag)


start_dummy >> run_this >> end_dummy
