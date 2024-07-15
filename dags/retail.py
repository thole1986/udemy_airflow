from airflow.decorators import dag, task
from datetime import datetime
from time import sleep


@dag(start_date=datetime(2024, 1, 1),
     schedule='@daily',
    #  catchup=False,
     catchup=True,
     tags=['test']
)
def retail():
    
    @task(retries=15)
    def start():
        # print('Hi')
        sleep(60*5)

    start()
    
retail()