import requests
import json
from airflow.hooks.base import BaseHook
from minio import Minio
from io import BytesIO


BUCKET_NAME = 'stock-market'


def _get_minio_client():
    minio = BaseHook.get_connection('minio')
    client = Minio(
        endpoint=minio.extra_dejson['endpoint_url'].split('//')[1],
        access_key=minio.login,
        secret_key=minio.password,
        secure=False,
    )
    return client


def _get_stock_prices(url, symbol):
    # https://query1.finance.yahoo.com/v8/finance/chart/
    url = f"{url}{symbol}?metrics=high?&interval=1d&range=1y"
    # The 'stock_api' is setup on airflow ui already
    api = BaseHook.get_connection('stock_api')
    response = requests.get(url, headers=api.extra_dejson['headers'])
    return json.dumps(
        response.json()['chart']['result'][0]
    )

def _store_prices(stock):
    client = _get_minio_client()
    
    client.bucket_exists(BUCKET_NAME)
    if not client.bucket_exists(BUCKET_NAME):
        client.make_bucket(BUCKET_NAME)
    
    # convert stock string into python dict
    stock = json.loads(stock)
    symbol = stock['meta']['symbol']
    data = json.dumps(stock, ensure_ascii=False).encode('utf8')
    objw = client.put_object(
        bucket_name=BUCKET_NAME,
        object_name=f'{symbol}/prices.json',
        data=BytesIO(data),
        length=len(data)
    )

    return f'{objw.bucket_name}/{symbol}'


def _get_formatted_csv(location):
    client = _get_minio_client()
    objects = client.list_objects(
        f'stock-market',
        prefix='AAPL/formatted_prices/',
        recursive=True
    )
    csv_file = [obj for obj in objects if obj.object_name.endswith('.csv')][0]
    return f's3://{csv_file.bucket_name}/{csv_file.object_name}'
