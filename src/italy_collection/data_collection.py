import os
import logging
import requests
import numpy as np
import pandas as pd

from tqdm import tqdm
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from src.clients.create_clients import get_db_connection


BATCH_SIZE = 100
START_WRITE_DATA_DATE = '2020-01-01'

TOKEN = os.getenv("ITALY_API_TOKEN")
STATIC_LINK = os.getenv("ITALY_SENSOR_API_URL", "https://portal.smart1.eu/export/data/csv/376/linear/")


class Read:
    def __init__(self, token, static_link, read_interval):
        self.token = token
        self.static_link = static_link
        self.read_interval = read_interval

    def read_data_api(self, sensor_id, last_date, name_sensor):
        name_sensor = self.name_to_format(name_sensor)
        str_date = last_date.strftime("%Y%m%d") + "/"
        api_link = f"{self.static_link}{self.read_interval}/detailed/{str_date}{sensor_id}?apikey={self.token}"
        # logging.info(api_link)
        print(api_link)
        response = requests.get(api_link)
        logging.info(f"API request to {api_link}, status: {response.status_code}")

        if response.status_code != 200:
            logging.error(f"Failed to fetch data: {response.text}")
            return pd.DataFrame(columns=["Timestamp", name_sensor])

        df = pd.read_csv(api_link, sep=';')
        df = df.rename(columns={'Value1': name_sensor})
        logging.info('='*100)
        logging.info(df)
        logging.info('='*100)
        return df

    def general_period(self, name_sensor, sensor_id, last_date):
        name_sensor = self.name_to_format(name_sensor)
        df_general_period = pd.DataFrame(columns=["Timestamp", name_sensor])
        df_general_period['Timestamp'] = pd.to_datetime(df_general_period['Timestamp'])
        df_general_period[name_sensor] = pd.to_numeric(df_general_period[name_sensor], errors='coerce')

        last_date = last_date.replace(tzinfo=None)
        today_date = datetime.today()

        while today_date > last_date:
            df_sensor = self.read_data_api(sensor_id, last_date, name_sensor)
            if df_sensor.empty:
                last_date += relativedelta(months=1)
                continue
            df_sensor = df_sensor.iloc[:-1]
            df_general_period = pd.concat([df_general_period, df_sensor], ignore_index=True)
            last_date += relativedelta(months=1)

        start_of_month = today_date.replace(day=1)
        df_sensor = self.read_data_api(sensor_id, start_of_month, name_sensor)
        df_sensor = df_sensor.iloc[:-1]
        df_general_period = pd.concat([df_general_period, df_sensor], ignore_index=True)

        df_general_period = df_general_period.rename(columns={'Timestamp': 'datetime'})
        df_general_period = df_general_period[['datetime', name_sensor]]
        df_general_period = df_general_period.drop_duplicates(subset=['datetime'], keep='first')

        return name_sensor, df_general_period

    def name_to_format(self, name):
        name = ''.join(['_' if not c.isalnum() else c for c in name]).rstrip('_')
        name = '_'.join(filter(None, name.split('_'))).lower()
        return name


def get_last_datetime():
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT MAX(datetime) FROM load_consumption;")
        result = cur.fetchone()[0]
        cur.close()
        conn.close()
        logging.info(f"Last known datetime: {result}")
        return result
    except Exception as e:
        logging.error(f"Ошибка при получении последней даты: {e}")
        raise


def fetch_sensor_data(last_date):
    try:
        if last_date is None:
            last_date = datetime.strptime(START_WRITE_DATA_DATE, '%Y-%m-%d')

        data = Read(
            token=TOKEN,
            static_link=STATIC_LINK,
            read_interval='month'
        )

        df = data.general_period(
            name_sensor="load_consumption",
            sensor_id='arithmetic_1464947681',
            last_date=last_date
        )

        if isinstance(df, tuple) and len(df) == 2:
            df = df[1]

        logging.info(f"Данные с сенсора успешно получены, строк: {len(df)}")
        return df
    except Exception as e:
        logging.error(f"Ошибка при получении данных с сенсора: {e}")
        raise


def prepare_data_to_upload(df_data_sensor, last_known_date):
    try:
        df_data_sensor['datetime'] = pd.to_datetime(df_data_sensor['datetime'])
        if last_known_date is not None:
            last_known_date = pd.to_datetime(last_known_date).tz_localize(None)
            df_to_upload = df_data_sensor[df_data_sensor['datetime'] > last_known_date]
        else:
            df_to_upload = df_data_sensor
        logging.info(f"Подготовлено {len(df_to_upload)} строк к загрузке")
        return df_to_upload
    except Exception as e:
        logging.error(f"Ошибка при подготовке данных к загрузке: {e}")
        raise


def upload_to_db(df_to_upload):
    try:
        if df_to_upload is None or df_to_upload.empty:
            logging.info("Нет данных для загрузки")
            return "Нет новых данных"

        df_to_upload = df_to_upload.dropna()
        total_rows = len(df_to_upload)

        logging.info(f"Загружаем {total_rows} строк в DB батчами по {BATCH_SIZE}")

        conn = get_db_connection()
        cur = conn.cursor()

        insert_query = """
        INSERT INTO load_consumption (datetime, load_consumption)
        VALUES (%s, %s)
        ON CONFLICT (datetime) DO UPDATE 
        SET load_consumption = EXCLUDED.load_consumption;
        """

        for i, chunk in tqdm(enumerate(np.array_split(df_to_upload, np.ceil(total_rows / BATCH_SIZE)))):
            data_to_insert = [tuple(row) for row in chunk.itertuples(index=False, name=None)]
            cur.executemany(insert_query, data_to_insert)
            conn.commit()
            logging.info(f"Загружено {min((i + 1) * BATCH_SIZE, total_rows)} / {total_rows} записей")
            print(f"Загружено {min((i + 1) * BATCH_SIZE, total_rows)} / {total_rows} записей")


        cur.close()
        conn.close()
        logging.info(f"Загрузка завершена: {total_rows} записей загружено")
        return f"{total_rows} записей загружено"

    except Exception as e:
        logging.error(f"Ошибка при загрузке в базу данных: {e}")
        raise


def run_sensor_pipeline():
    try:
        last_known_date = get_last_datetime()
        raw_data = fetch_sensor_data(last_known_date)
        prepared_data = prepare_data_to_upload(raw_data, last_known_date)
        result = upload_to_db(prepared_data)
        return result
    except Exception as e:
        logging.error(f"Ошибка в pipeline: {e}")
        raise
