import psycopg2
from fastapi import FastAPI
from datetime import datetime, timezone
from datetime import timedelta
from pandas import DataFrame
import pandas as pd
from tinkoff.invest import Client
from dotenv import load_dotenv
import os
import json

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


def get_candle_date_from_db(data:list) -> dict:
    dict_figi_time = []

    for share in data:
        dict_figi_time.append(share.first_1min_candle_date.replace(tzinfo=None))

    return dict_figi_time


with Client(TINKOFF_TOKEN) as client:
    shares_connection = client.instruments.shares()


candle_date = get_candle_date_from_db(shares_connection.instruments)

a=1



