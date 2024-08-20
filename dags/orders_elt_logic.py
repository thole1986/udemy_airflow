import pandas as pd
import sqlalchemy as db

orders_table = '/usr/local/airflow/dags/H+ Sport orders.xlsx'
engine = db.create_engine('postgresql://rufcjwsw:yl6y6OUUeWr1q67puO54RwyMoMFq8Mbc@batyr.db.elephantsql.com/rufcjwsw')

def main():
    orders = pd.read_excel(orders_table, sheet_name='data', engine='openpyxl')
    orders = orders[
        ["OrderID","Date","TotalDue","Status", "CustomerID","SalespersonID", "CustomersComment"]
    ]

    orders.to_sql(
        'orders',
        engine,
        if_exists='replace',
        index=False,
    )

print("ETL script run successfully!")


