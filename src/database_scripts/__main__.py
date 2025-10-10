from datetime import datetime
from tinkoff.invest import Client,CandleInterval
import threading
from src.database_scripts.tinkoff_actions import TINKOFF_TOKEN, get_figi_from_file, get_lonely_figi_data, \
    fetch_and_write_data_from_db, process_all_stocks_multithreaded, safe_print, check_table_exists

from tinkoff.invest import Client


if __name__ == '__main__':
    #figi_list = get_figi_from_file('candles_info_popular_ru_us.csv')
    #check_tinkoff_simple()
    #igi_list = figi_list[:2]

    #data1 = fetch_data_from_db('t1', 4, 'BBG00F6NKQX3')
    #data1 = fetch_data_from_db('t2', SIZE_PACKAGE_DAYS, 'BBG000VKG4R5')

    # Получаем список FIGI из файла
    figi_list = get_figi_from_file('candles_info_popular_ru_us.csv')

    #figi_list = ['BBG00F6NKQX3', 'BBG000VKG4R5', 'BBG000BNSZP1']#figi_list[:2]
    #figi_list = figi_list[:6]
    # Создаем конфигурацию для каждой акции вида имя_файла, размер пакета
    stocks_config = []
    SIZE_PACKAGE_DAYS = 4
    for i, figi in enumerate(figi_list):
        table_name = f'{figi}'
        stocks_config.append((table_name, SIZE_PACKAGE_DAYS))  # (table_name, days, figi)

    process_all_stocks_multithreaded(stocks_config, max_workers=3)

    #check_table_exists('BBG00F6NKQX3')

    a=1

