import csv
from pathlib import Path
from typing import Any

import psycopg2
from datetime import timedelta
from pandas import DataFrame
import pandas as pd
from tinkoff.invest import Client, CandleInterval
from dotenv import load_dotenv
import os
from tinkoff.invest.caching.market_data_cache.cache import MarketDataCache
from tinkoff.invest.caching.market_data_cache.cache_settings import MarketDataCacheSettings
from tinkoff.invest.utils import now

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


def get_cached_candles_data(figi_list: list, days: int, interval: CandleInterval) -> DataFrame:
    """
    figi_list: список идентификаторов figi акций
    days: кол-во дней исторических данных для загрузки. Глубина истории
    interval: интервал свечей

    return candles_df:
        DataFrame: Датафрейм с колонками:
            - figi: идентификатор инструмента
            - time: время свечи
            - open: цена открытия
            - high: максимальная цена
            - low: минимальная цена
            - close: цена закрытия
            - volume: объем
            - is_complete: завершена ли свеча
    """
    all_candles = []

    with Client(TINKOFF_TOKEN) as client:
        settings = MarketDataCacheSettings(base_cache_dir=Path("market_data_cache"))
        market_data_cache = MarketDataCache(settings=settings, services=client)

        for figi in figi_list:
            try:
                candles_list = list(market_data_cache.get_all_candles(
                    figi=figi,
                    from_=now() - timedelta(days=days),
                    interval=interval,
                ))

                for candle in candles_list:
                    candle_data = {
                        'figi': figi,
                        'time': candle.time,
                        'open': float(candle.open.units) + float(candle.open.nano) / 1e9,
                        'high': float(candle.high.units) + float(candle.high.nano) / 1e9,
                        'low': float(candle.low.units) + float(candle.low.nano) / 1e9,
                        'close': float(candle.close.units) + float(candle.close.nano) / 1e9,
                        'volume': candle.volume,
                        'is_complete': candle.is_complete
                    }
                    all_candles.append(candle_data)

                print(f"Получено данных для {figi}: {len(candles_list)} свечей")

            except Exception as e:
                print(f"Ошибка в ф-и get_cached_candles_data(...) \n для FIGI {figi}: {e} ")

    candles_df = pd.DataFrame(all_candles)

    if not candles_df.empty:
        candles_df = candles_df.sort_values(['figi', 'time']).reset_index(drop=True)

    return candles_df


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







