from fastapi import FastAPI
from datetime import datetime
from datetime import timedelta
from pandas import DataFrame


TINKOFF_TOKEN = '???'
DB_URL = '???'

def get_last_datetime(db):
    return

def get_new_data(last_db_datetime: datetime, current_datetime: datetime):
    return

def handle_data_filtration(db: DataFrame):
    return

def add_in_database(db: DataFrame):
    pass


current_date = datetime.today()
current_date = current_date.strftime('%d.%m.%y')

endpoint = FastAPI()




