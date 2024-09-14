import json
from io import BytesIO
from airflow.exceptions import AirflowNotFoundException
import requests
from airflow.hooks.base import BaseHook
from minio import Minio

bucket_name = 'stock-market-dev'


def _get_minio_client():
    minio = BaseHook.get_connection('minio')
    client = Minio(
        endpoint=minio.extra_dejson['endpoint_url'].split('//')[1],
        access_key=minio.login,
        secret_key=minio.password,
        secure=False
    )
    return client

def _get_stock_prices(url, symbol):
    url = f"{url}{symbol}?metrics=high?&interval=1d&range=1y"
    api = BaseHook.get_connection('stock_api')
    response = requests.get(url, headers=api.extra_dejson['headers'])
    return json.dumps(response.json()['chart']['result'][0])

def _store_prices(stock):
    client = _get_minio_client()
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
        print("Created bucket", bucket_name)
    else:
        print("Bucket", bucket_name, "already exists")
    stock = json.loads(stock)
    symbol = stock['meta']['symbol']
    data = json.dumps(stock, ensure_ascii=False).encode('utf8')
    objw = client.put_object(
        bucket_name=bucket_name,
        object_name=f'{symbol}/prices.json',
        data=BytesIO(data),
        length=len(data)
    )
    return f'{objw.bucket_name}/{symbol}'
# get minio latest verion https://pypi.org/project/minio/#history


def _get_formatted_csv(path):
    client = _get_minio_client()
    # prefix_name = f"{path.split('/')}/formatted_prices/"
    prefix_name = f"{path.split('/')[1]}/formatted_prices/"
    print(f'path: {path} and prefix {prefix_name}')

    # List objects information.
    objects = client.list_objects(bucket_name, prefix=prefix_name, recursive=True)
    for obj in objects:
        print(obj.bucket_name, obj.object_name, obj.last_modified, obj.etag, obj.size, obj.content_type)
        if obj.object_name.endswith('.csv'):
            print('csv file ')
            return obj.object_name
    raise AirflowNotFoundException('The csv file does not exist')


# this function is used to debug the return path of the above function
# def _print_any(path):
#     print(f'path s3://{bucket_name}/{path}')
