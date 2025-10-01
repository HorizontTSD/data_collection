import concurrent
import csv
import datetime
import time
from pathlib import Path
from typing import Any
import psycopg2
from datetime import timedelta
from pandas import DataFrame
import pandas as pd
from tinkoff.invest import Client, CandleInterval, InstrumentIdType
from dotenv import load_dotenv
import os
from tinkoff.invest.caching.market_data_cache.cache import MarketDataCache
from tinkoff.invest.caching.market_data_cache.cache_settings import MarketDataCacheSettings
from tinkoff.invest.utils import now
from enum import Enum
import threading


load_dotenv()
TINKOFF_TOKEN = os.getenv('TINKOFF_TOKEN')

print_lock = threading.Lock()

def safe_print(message:str):
    with print_lock:
        print(message)


def fetch_data_from_db(table_name: str, size_package_days: int, candle_name: str) -> DataFrame:

    columns_name = ["datetime", "load_consumption"]
    columns_name_str = ''
    for column in columns_name:
        columns_name_str += column + ', '
    columns_name_str = columns_name_str[:-2]

    DB_PARAMS = {
        "dbname": os.getenv("DB_NAME"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "host": os.getenv("DB_HOST"),
        "port": int(os.getenv("DB_PORT"))
    }
    '''
    conn = psycopg2.connect(**DB_PARAMS)
    cur = conn.cursor()

    try:
        select_query = f"""
        SELECT {columns_name_str} 
        FROM {table_name} 
        ORDER BY datetime;
        """

        cur.execute(select_query)
        rows = cur.fetchall()

        df_result = pd.DataFrame(rows, columns=columns_name)
        df_result["datetime"] = df_result["datetime"].dt.tz_localize(None)
    '''

    try:

        df_result = pd.read_csv(f'{table_name}.csv')
        df_result = df_result.drop_duplicates()

        with Client(TINKOFF_TOKEN) as client:
            last_time = pd.to_datetime(df_result.values[-1][0]).to_pydatetime()
            df_new = get_lonely_figi_data(client, candle_name, CandleInterval.CANDLE_INTERVAL_1_MIN,size_package_days, last_time)

            df_new[1:].to_csv(f'{table_name}.csv',
                          mode='a',
                          index=False, header=False)

        print(f'I EXIST! {len(df_result)} rows')

    except:

        '''
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {columns_name[0]} TIMESTAMP PRIMARY KEY,
            {columns_name[1]} FLOAT
        );
        """
        '''
        a=1
        with Client(TINKOFF_TOKEN) as client:
            df_result = get_lonely_figi_data(client, candle_name, CandleInterval.CANDLE_INTERVAL_1_MIN, size_package_days, None)

        df_result.to_csv(f'{table_name}.csv', index=False)
        print(f'CREATE NEW TABLE {len(df_result)} rows')


    #cur.close()
    #conn.close()

    return df_result



def get_lonely_figi_data(client, figi: str, candle_interval: CandleInterval, size_package_days: int,
                         last_time:datetime.datetime) -> DataFrame:

    if not isinstance(candle_interval, CandleInterval):
        raise TypeError(f"candle_interval должен быть CandleInterval, получен {type(candle_interval)}")

    valid_intervals = {
        CandleInterval.CANDLE_INTERVAL_1_MIN: 'first_1min_candle_date',
        CandleInterval.CANDLE_INTERVAL_5_MIN: 'first_5min_candle_date'
    }

    if candle_interval not in valid_intervals:
        raise ValueError(f"Неподдерживаемый интервал: {candle_interval}")

    try:
        instrument = client.instruments.get_instrument_by(
            id_type=InstrumentIdType.INSTRUMENT_ID_TYPE_FIGI,
            id=figi
        ).instrument

        first_candle_attr = valid_intervals[candle_interval]
        first_time = getattr(instrument, first_candle_attr, None)

        if not first_time:
            raise ValueError(f"Нет данных о первой свече для интервала {candle_interval}")

        if last_time and last_time.tzinfo is None:
            last_time = last_time.replace(tzinfo=datetime.timezone.utc)

        target_from = last_time if last_time else first_time
        target_to = target_from + timedelta(days=size_package_days)

        max_period = timedelta(days=1)
        if target_to - target_from > max_period:
            target_to = target_from + max_period

        candles_response = client.market_data.get_candles(
            figi=figi,
            from_=target_from,
            to=target_to,
            interval=candle_interval
        )

        data = []
        for candle in candles_response.candles:
            data.append({
                'datetime': candle.time.replace(tzinfo=None).isoformat(sep=' ', timespec='seconds'),
                'open': float(candle.open.units) + float(candle.open.nano) / 1e9,
                'high': float(candle.high.units) + float(candle.high.nano) / 1e9,
                'low': float(candle.low.units) + float(candle.low.nano) / 1e9,
                'close': float(candle.close.units) + float(candle.close.nano) / 1e9,
                'volume': candle.volume,
                'figi': figi
            })

        print(f"{figi}: получено {len(data)} свечей")

    except Exception as err:
        data = []
        print(f"{figi}: {err}")

    return pd.DataFrame(data)


def get_all_figi_from_tbank(data:list) -> list[Any]:
    """
    data: instruments из тинькоффа с данными об акциях
    return figi: список идентификаторов акций figi
    """
    figi = []

    for candle in data.instruments:
        figi.append(candle.figi)
    """
    # побочный квест с созданием первичного файла со всеми возможными акциями |figi:название акции|
    data_sorted = sorted(data.instruments, key=lambda k: k.name)
    with open('candles_info.csv', 'w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)

        writer.writerow(['FIGI', 'Name'])

        for candle in data_sorted:
            writer.writerow([candle.figi, candle.name])
    """
    return figi


def get_figi_from_file(file_name:str) -> list:
    """
    file_name: имя файла с отобранными вручную акциями (из побочного квеста)
    return figi: список уникальных id избранных акций
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(script_dir, file_name)

    figi_list = []
    with open(file_path, 'r', encoding='utf-8') as file:
        csv_reader = csv.reader(file)
        next(csv_reader)

        for row in csv_reader:
            if row:
                figi_list.append(row[0])

    return figi_list

def process_single_stock(args):
    """Обрабатывает одну акцию в потоке"""
    table_name, size_package_days, candle_name = args
    try:
        safe_print(f"Обрабатываем {candle_name}...")
        result = fetch_data_from_db(table_name, size_package_days, candle_name)
        result = result.drop_duplicates()
        safe_print(f"Акция {candle_name}: завершено")
        return True
    except Exception as e:
        safe_print(f"Ошибка {candle_name}: {e}")
        return False


def process_all_stocks_multithreaded(stocks_config, max_workers=3):
    """
    stocks_config: список кортежей (table_name, size_package_days, candle_name)
    max_workers: количество потоков (рекомендуется 3-5)
    """
    safe_print(f"Запуск многопоточности для {len(stocks_config)} акций")

    start_time = time.time()

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        results = list(executor.map(process_single_stock, stocks_config))

    successful = sum(results)
    total_time = time.time() - start_time

    safe_print(f"Завершено: {successful}/{len(stocks_config)} акций успешно")
    safe_print(f"Общее время: {total_time:.1f} сек")

    return successful