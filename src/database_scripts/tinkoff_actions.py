import concurrent
import csv
import datetime
import logging
import time
from typing import Any
import psycopg2
from datetime import timedelta
from pandas import DataFrame
import pandas as pd
from tinkoff.invest import Client, CandleInterval, InstrumentIdType
from dotenv import load_dotenv
import os
import threading


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('app.log'),  # Запись в файл app.log
        logging.StreamHandler()          # Вывод в консоль
    ]
)

load_dotenv()
TINKOFF_TOKEN = os.getenv('TINKOFF_TOKEN')

print_lock = threading.Lock() # замок для безопасной печати

def safe_print(message:str):
    """
    безопасная печать для многопоточности
    """
    with print_lock:
        logging.info(f'{message}')


def check_table_exists(table_name: str) -> bool:
    """Проверяет существование таблицы в БД"""

    DB_PARAMS = {
        "dbname": os.getenv("PG_DB_TEST"),
        "user": os.getenv("PG_USER_TEST"),
        "password": os.getenv("PG_PASSWORD_TEST"),
        "host": os.getenv("PG_HOST_TEST"),
        "port": int(os.getenv("PG_PORT_TEST"))
    }

    try:
        conn = psycopg2.connect(**DB_PARAMS)
        cur = conn.cursor()

        # PostgreSQL сохраняет имена таблиц в нижнем регистре!!!
        table_name_lower = table_name.lower()

        check_query = """
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name = %s
        );
        """

        cur.execute(check_query, (table_name_lower,))
        exists = cur.fetchone()[0]

        cur.close()
        conn.close()

        safe_print(
            f"Таблица {table_name} (ищем как {table_name_lower}): {'СУЩЕСТВУЕТ' if exists else 'НЕ СУЩЕСТВУЕТ'}")
        return exists

    except Exception as e:
        safe_print(f"ERROR!: Ошибка при проверке таблицы: {e}")
        return False


def fetch_and_write_data_from_db(table_name: str, size_package_days: int) -> DataFrame:
    """
    Извлекает данные из timescale db и в случае необходимости создает таблицу по каждой отдельной акции
    table_name: имя таблицы состоит из figi каждой акции
    size_package_days: размер пакета данных в днях
    """
    DB_PARAMS = {
        "dbname": os.getenv("PG_DB_TEST"),
        "user": os.getenv("PG_USER_TEST"),
        "password": os.getenv("PG_PASSWORD_TEST"),
        "host": os.getenv("PG_HOST_TEST"),
        "port": int(os.getenv("PG_PORT_TEST"))
    }

    conn = psycopg2.connect(**DB_PARAMS)
    cur = conn.cursor()
    df_result = pd.DataFrame()

    try:
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = %s
            );
        """, (table_name.lower(),))

        table_exists = cur.fetchone()[0]

        if table_exists:
            # Таблица существует - читаем данные
            select_query = (f"SELECT datetime, open, high, low, close, volume, figi "
                            f"FROM {table_name} ORDER BY datetime;")

            cur.execute(select_query)

            rows = cur.fetchall()
            if rows:
                df_result = pd.DataFrame(rows, columns=["datetime", "open", "high", "low", "close", "volume", "figi"])
                df_result["datetime"] = df_result["datetime"].dt.tz_localize(None)

            safe_print(f'Загружено из таблицы "{table_name.lower()}": {len(df_result)} строк')

            # Получаем новые данные
            with Client(TINKOFF_TOKEN) as client:
                if not df_result.empty:
                    last_time = pd.to_datetime(df_result["datetime"].max()).to_pydatetime()
                else:
                    last_time = None

                df_new = get_lonely_figi_data(client, table_name, CandleInterval.CANDLE_INTERVAL_1_MIN,
                                              size_package_days, last_time)

                # Добавляем новые данные в БД
                if not df_new.empty:
                    df_new = df_new.drop_duplicates(subset=['datetime'])

                    for _, row in df_new.iterrows():
                        insert_query = f"""
                            INSERT INTO {table_name} (datetime, open, high, low, close, volume, figi)
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (datetime) DO UPDATE SET
                                open = EXCLUDED.open,
                                high = EXCLUDED.high, 
                                low = EXCLUDED.low,
                                close = EXCLUDED.close,
                                volume = EXCLUDED.volume,
                                figi = EXCLUDED.figi;
                        """
                        cur.execute(insert_query, (
                            row['datetime'], row['open'], row['high'], row['low'],
                            row['close'], row['volume'], row['figi']
                        ))

                    safe_print(f'Таблица "{table_name.lower()}" обновлена: +{len(df_new)} строк')
                else:
                    safe_print(f'Нет новых данных для таблицы "{table_name.lower()}"')

        else:
            # Таблица не существует - создаем и заполняем
            safe_print(f'Создаем таблицу {table_name} в БД')

            create_table_query = f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    datetime TIMESTAMP PRIMARY KEY,
                    open FLOAT NOT NULL,
                    high FLOAT NOT NULL,
                    low FLOAT NOT NULL,
                    close FLOAT NOT NULL,
                    volume BIGINT NOT NULL,
                    figi VARCHAR(20) NOT NULL
                );
            """
            cur.execute(create_table_query)
            conn.commit()

            # Получаем начальные данные
            with Client(TINKOFF_TOKEN) as client:
                df_result = get_lonely_figi_data(client, table_name, CandleInterval.CANDLE_INTERVAL_1_MIN,
                                                 size_package_days, None)
            # Записываем данные в новую таблицу
            if not df_result.empty:
                df_result = df_result.drop_duplicates(subset=['datetime'])
                for _, row in df_result.iterrows():
                    insert_query = f"""
                        INSERT INTO {table_name} (datetime, open, high, low, close, volume, figi)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (datetime) DO NOTHING;
                    """
                    cur.execute(insert_query, (
                        row['datetime'], row['open'], row['high'], row['low'],
                        row['close'], row['volume'], row['figi']
                    ))
                safe_print(f'Таблица "{table_name}" успешно создана. В ней {len(df_result)} строк')
            else:
                safe_print(f'Не удалось получить данные для создания таблицы')

        conn.commit()

    except Exception as e:
        conn.rollback()  # Откатываем при ошибке
        safe_print(f'ERROR!: ОШИБКА БД {table_name}: {e}')

    finally:
        cur.close()
        conn.close()

    return df_result


def get_lonely_figi_data(client, figi: str, candle_interval: CandleInterval, size_package_days: int,
                         last_time:datetime.datetime) -> DataFrame:
    """
    client: клиент тинькоффа
    figi: уникальный идентификатор акции. название таблицы это фиги в нижнем регистре
    candle_interval: интервал свечи
    size_package_days: размер пакета данных в днях
    last_time:
        None - нет записаей по акции
        Время последней записи в таблице по акции
    """

    if not isinstance(candle_interval, CandleInterval):
        raise TypeError(f"candle_interval должен быть CandleInterval, получен {type(candle_interval)}")

    valid_intervals = {
        CandleInterval.CANDLE_INTERVAL_1_MIN: 'first_1min_candle_date',
        CandleInterval.CANDLE_INTERVAL_5_MIN: 'first_5min_candle_date'
    }

    if candle_interval not in valid_intervals:
        raise ValueError(f"Неподдерживаемый интервал: '{candle_interval}'")

    try:
        instrument = client.instruments.get_instrument_by(
            id_type=InstrumentIdType.INSTRUMENT_ID_TYPE_FIGI,
            id=figi
        ).instrument

        first_candle_attr = valid_intervals[candle_interval]
        first_time = getattr(instrument, first_candle_attr, None)

        if not first_time:
            raise ValueError(f"Нет данных о первой свече для интервала '{candle_interval}'")

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

        safe_print(f"Из акции с идентификатором '{figi}': получено {len(data)} свечей\n")

    except Exception as err:
        data = []
        safe_print(f"ERROR!: {figi}: {err}")

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
    """Обрабатывает одну акцию в потоке
    args:
        candle_name - figi акции
        size_package_days - размер пакета в днях
    """
    candle_name, size_package_days = args
    try:
        safe_print(f"Обрабатываем акцию '{candle_name}'...")
        result = fetch_and_write_data_from_db(candle_name, size_package_days)
        result = result.drop_duplicates()
        time.sleep(0.5)
        #check_table_exists(candle_name)
        safe_print(f"Акция '{candle_name}': завершено")
        return True

    except Exception as e:
        safe_print(f"ERROR!: Ошибка '{candle_name}': {e}")
        return False


def process_all_stocks_multithreaded(stocks_config, max_workers=3):
    """
    stocks_config: список кортежей (candle_name, size_package_days)
    max_workers: количество потоков (рекомендуется 3-5)
    """
    safe_print(f"Запуск многопоточности для {len(stocks_config)} акций...")

    start_time = time.time()

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        results = list(executor.map(process_single_stock, stocks_config))

    successful = sum(results)
    total_time = time.time() - start_time

    safe_print(f"Завершено: {successful}/{len(stocks_config)} акций успешно")
    safe_print(f"Общее время: {total_time:.1f} сек")

    return successful