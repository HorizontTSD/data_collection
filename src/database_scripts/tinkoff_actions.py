import csv
import datetime
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

load_dotenv()
TINKOFF_TOKEN = os.getenv('TINKOFF_TOKEN')


def fetch_data_from_db(table_name: str = 'kload_consumption') -> DataFrame:

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
            df_result = get_lonely_figi_data(client, 'BBG00F6NKQX3', CandleInterval.CANDLE_INTERVAL_1_MIN,3, last_time)

            df_result.to_csv(f'{table_name}.csv',
                          mode='a',
                          index=False)

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
            df_result = get_lonely_figi_data(client, 'BBG00F6NKQX3', CandleInterval.CANDLE_INTERVAL_1_MIN, 3)

        df_result.to_csv(f'{table_name}.csv', index=False)
        print(f'CREATE NEW TABLE {len(df_result)} rows')


    #cur.close()
    #conn.close()

    return df_result


def get_cached_candles_data(figi_list: list, size_package_days: int, candle_interval: CandleInterval) -> DataFrame:


    '''
    all_candles = []

    with Client(TINKOFF_TOKEN) as client:
        settings = MarketDataCacheSettings(base_cache_dir=Path("market_data_cache"))
        market_data_cache = MarketDataCache(settings=settings, services=client)

        for figi in figi_list:
            #TODO эта херня мне не нравится. переделать
            first_time = client.instruments.get_instrument_by(
                id_type=InstrumentIdType.INSTRUMENT_ID_TYPE_FIGI,
                id=figi).instrument.first_1day_candle_date

            try:
                time_line = now() - first_time
                if time_line >= timedelta(days=days):
                    time_line = first_time + timedelta(days=7)

                candles_list = list(market_data_cache.get_all_candles(
                    figi=figi,
                    #TODO изменить диапазон
                    from_=time_line,
                   # from_=now() - timedelta(days=days),
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
                    if candle_data not in all_candles:
                        all_candles.append(candle_data)

                print(f"Получено данных для {figi}: {len(candles_list)} свечей")

            except Exception as e:
                print(f"Ошибка в ф-и get_cached_candles_data(...) \n для FIGI {figi}: {e} ")

    candles_df = pd.DataFrame(all_candles)

    if not candles_df.empty:
        candles_df = candles_df.sort_values(['figi', 'time']).reset_index(drop=True)
    '''
    return # candles_df


def get_lonely_figi_data(client, figi: str, candle_interval: CandleInterval, size_package_days: int,
                         last_time:datetime.datetime = None) -> DataFrame:
    '''
    client: клиент тинькоффа
    candle_interval: интервал свечей
    size_package_days: размер пакета данных в днях
    last_time: время последней записи в базе данных. если None - то записи не были сделаны и будет считано самое первое время свечи

    return candles_df:
        DataFrame: Датафрейм с колонками:
            - datetime: время свечи
            - open: цена открытия
            - high: максимальная цена
            - low: минимальная цена
            - close: цена закрытия
            - volume: объем
            - is_complete: завершена ли свеча
            - figi: идентификатор инструмента
    """
    '''

    if not isinstance(candle_interval, CandleInterval):
        raise TypeError(f"candle_interval должен быть CandleInterval, получен {type(candle_interval)}")

    valid_intervals = {
        CandleInterval.CANDLE_INTERVAL_1_MIN: 'first_1min_candle_date',
        CandleInterval.CANDLE_INTERVAL_5_MIN: 'first_5min_candle_date'
    }

    if candle_interval not in valid_intervals:
        raise ValueError(f"Неподдерживаемый интервал: {candle_interval}")

    settings = MarketDataCacheSettings(base_cache_dir=Path("market_data_cache"))
    market_data_cache = MarketDataCache(settings=settings, services=client)

    try:
        instrument = client.instruments.get_instrument_by(
            id_type=InstrumentIdType.INSTRUMENT_ID_TYPE_FIGI,
            id=figi
        ).instrument

        first_candle_attr = valid_intervals[candle_interval]
        first_time = getattr(instrument, first_candle_attr, None)

        if not first_time:
            # в 5 минутных интервалах часто такое будет. скипаем
            raise ValueError(f"Нет данных о первой свече для интервала {candle_interval}")

        if last_time and last_time.tzinfo is None:
            last_time = last_time.replace(tzinfo=datetime.timezone.utc)

        target_from = last_time if last_time else first_time
        target_to = target_from + timedelta(days=size_package_days)

        candles_iterator = market_data_cache.get_all_candles(
            figi=figi,
            from_=target_from,
            to=target_to,
            interval=candle_interval
        )

        try:
            candles_list = list(candles_iterator)

            if candles_list:
                print(f"DEBUG: First candle time = {candles_list[0].time}")
                print(f"DEBUG: Last candle time = {candles_list[-1].time}")
            else:
                print("DEBUG: candles_list is empty!")

        except Exception as e:
            print(f"DEBUG: Error converting to list: {e}")
            candles_list = []

        data = []
        for candle in candles_list:
            data.append({
                'datetime': candle.time.replace(tzinfo=None).isoformat(sep=' ', timespec='seconds'),
                'open': float(candle.open.units) + float(candle.open.nano) / 1e9,
                'high': float(candle.high.units) + float(candle.high.nano) / 1e9,
                'low': float(candle.low.units) + float(candle.low.nano) / 1e9,
                'close': float(candle.close.units) + float(candle.close.nano) / 1e9,
                'volume': candle.volume,
                'figi': figi
            })

    except Exception as err:
        data = {}
        print(err)

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







