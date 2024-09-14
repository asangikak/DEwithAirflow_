from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.sensors.base import PokeReturnValue
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.slack.notifications.slack_notifier import SlackNotifier
from datetime import datetime
import requests
from astro import sql as aql
from astro.files import File
from astro.sql.table import Table, Metadata

from include.stock_market.tasks import _get_stock_prices, _store_prices, _get_formatted_csv, bucket_name
symbol='AAPL'
@dag(
    start_date=datetime(2023,1,1),
    schedule='@daily',
    catchup=False,
    tags=['stock_market'],
    on_success_callback=SlackNotifier(
        slack_conn_id='slack',
        text='The DAG stock_market has succeeded',
        channel='general'
    ),
    on_failure_callback=SlackNotifier(
        slack_conn_id='slack',
        text='The DAG stock_market has failed',
        channel='general'
    )
)
def stock_market():
    @task.sensor(poke_interval=30, timeout=300, mode='poke')
    def is_api_available() -> PokeReturnValue:
        api = BaseHook.get_connection('stock_api')
        url = f"{api.host}{api.extra_dejson['endpoint']}"
        response = requests.get(url, headers=api.extra_dejson['headers'])
        condition= response.json()['finance']['result'] is None
        return PokeReturnValue(is_done=condition, xcom_value=url)

    get_stock_prices = PythonOperator(
        task_id='get_stock_prices',
        python_callable=_get_stock_prices,
        op_kwargs={'url': '{{ task_instance.xcom_pull(task_ids="is_api_available")}}', 'symbol': symbol}
    )
    store_prices = PythonOperator(
        task_id='store_prices',
        python_callable=_store_prices,
        op_kwargs={'stock': '{{ task_instance.xcom_pull(task_ids="get_stock_prices")}}'}

    )

    format_prices = DockerOperator(
        task_id='format_prices',
        image='airflow/stock-app',
        container_name='format_prices',
        api_version='auto',
        auto_remove=True,
        docker_url='tcp://docker-proxy:2375',
        network_mode='container:spark-master',
        tty=True,
        xcom_all=False,
        mount_tmp_dir=False,
        environment={
            'SPARK_APPLICATION_ARGS': '{{ task_instance.xcom_pull(task_ids="store_prices")}}'
        }
    )
    get_formatted_csv = PythonOperator(
        task_id='get_formatted_csv',
        python_callable=_get_formatted_csv,
        op_kwargs={'path': '{{ task_instance.xcom_pull(task_ids="store_prices")}}'}
    )

    # print_any = PythonOperator(
    #     task_id='print_any',
    #     python_callable=_print_any,
    #     op_kwargs={'path': '{{ task_instance.xcom_pull(task_ids="get_formatted_csv")}}'}
    # )

    # https://min.io/docs/minio/linux/developers/python/API.html
    # https://astro-sdk-python.readthedocs.io/en/stable/astro/sql/operators/load_file.html
    # Airflow - How to pass xcom variable into Python function . somehow the string concatenation was not working.
    # when I hardcoded the csv path it worked. so changed string concatenation in path

    load_to_dw = aql.load_file(
        task_id='load_to_dw',
        input_file=File(path="s3://{}/{}".format(bucket_name, '{{ task_instance.xcom_pull(task_ids="get_formatted_csv")}}'), conn_id='minio'),
        output_table=Table(
            name='stock_market',
            conn_id='postgres',
            metadata=Metadata(
                schema='public'
            )
        ),
        if_exists="replace"
    )
    is_api_available() >> get_stock_prices >> store_prices >> format_prices >> get_formatted_csv >> load_to_dw


stock_market()