from fastapi import FastAPI
from datetime import datetime, timezone
from datetime import timedelta
from pandas import DataFrame
import pandas as pd
from tinkoff.invest import Client
from dotenv import load_dotenv
import os

load_dotenv()
TINKOFF_TOKEN = os.getenv('TINKOFF_TOKEN')

DB_PARAMS = {
    "dbname": "mydb",
    "user": "myuser",
    "password": "mypassword",
    "host": "77.37.136.11",
    "port": 8083
}

def get_last_datetime(db):
    last_datetime = datetime(1000,1,1)
    for share in db:
        if share.first_1day_candle_date > last_datetime.replace(tzinfo=timezone.utc) :
            last_datetime = share.first_1day_candle_date

    return last_datetime

def get_new_data(last_db_datetime: datetime, current_datetime: datetime, db):
    new_data = []
    if current_datetime >= last_db_datetime:
        for share in db:
            if share.first_1day_candle_date == last_db_datetime:
                new_data.append(share)
    else:
        pass

    return new_data

def handle_data_filtration(df: DataFrame):
    df = df.drop_duplicates(subset=["figi"])
    # ?

    return df

def add_in_database(db: DataFrame):
    pass


current_date = datetime.today().replace(tzinfo=timezone.utc)

endpoint = FastAPI()

with Client(TINKOFF_TOKEN) as client:
    shares_db = client.instruments.shares()

last_datetime_share = get_last_datetime(shares_db.instruments)
fresh_data = get_new_data(last_datetime_share, current_date, shares_db.instruments)
fresh_data = pd.DataFrame(fresh_data)

df_unique = handle_data_filtration(fresh_data)

a=1



