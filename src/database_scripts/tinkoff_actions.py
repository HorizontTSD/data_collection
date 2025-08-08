import psycopg2
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


def fetch_data_from_db() -> DataFrame:
    table_name = 'load_consumption'
    measurement = 'load_consumption'

    DB_PARAMS = {
        "dbname": os.getenv("DB_NAME"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "host": os.getenv("DB_HOST"),
        "port": int(os.getenv("DB_PORT"))
    }
    conn = psycopg2.connect(**DB_PARAMS)
    cur = conn.cursor()

    select_query = f"""
    SELECT * FROM {table_name} ORDER BY datetime;
    """

    cur.execute(select_query)
    rows = cur.fetchall()

    df_result = pd.DataFrame(rows, columns=["datetime", measurement])
    df_result["datetime"] = df_result["datetime"].dt.tz_localize(None)

    cur.close()
    conn.close()

    return df_result



def get_last_datetime(data:list) -> datetime:
    # last_datetime = datetime(1000,1,1)
    dict_figi_time = {}

    # TODO на выходе словарь ключ-акция: время записи1candle в формате json
    for share in data:
        dict_figi_time[share.figi] = share.first_1min_candle_date


# при чем тут нижнее? json - просто словарь?

        #if share.first_1min_candle_date > last_datetime.replace(tzinfo=timezone.utc) :
        #    last_datetime = share.first_1min_candle_date
        #else:
          #  pass

    return last_datetime

def get_new_data(last_db_datetime: datetime, current_datetime: datetime, db):
    #TODO все к черту
    # тинькоф- бд поискать в документации.
    # dbparams - наше. не коммитить логины. из
    # как вытащить дагнные по конкретной акции.
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
    #df.to_csv('data.csv', encoding='utf-8')
    return df

def add_in_database(db: DataFrame):
    pass


current_date = datetime.today().replace(tzinfo=timezone.utc)
endpoint = FastAPI()

with Client(TINKOFF_TOKEN) as client:
    shares_connection = client.instruments.shares()




#df = fetch_data_from_db()
#df_init = df[:-288]
#df_test = df[-288:]


last_datetime_share = get_last_datetime(shares_connection.instruments)
'''
fresh_data = get_new_data(last_datetime_share, current_date, shares_connection.instruments)
fresh_data = pd.DataFrame(fresh_data)

df_unique = handle_data_filtration(fresh_data)
'''
a=1



