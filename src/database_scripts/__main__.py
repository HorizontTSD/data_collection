import time
from src.database_scripts.tinkoff_actions import get_figi_from_file, get_lonely_figi_data, \
    fetch_and_write_data_from_db, process_all_stocks_multithreaded, safe_print, check_table_exists

SIZE_PACKAGE_DAYS = 4


if __name__ == '__main__':
    figi_list = get_figi_from_file('candles_info_popular_ru_us.csv')

    while True:
        # Создаем конфигурацию для каждой акции вида имя_файла, размер пакета
        stocks_config = []
        for i, figi in enumerate(figi_list):
            table_name = f'{figi}'
            stocks_config.append((table_name, SIZE_PACKAGE_DAYS))  # (table_name, days, figi)

        process_all_stocks_multithreaded(stocks_config, max_workers=3)
        time.sleep(86400) # 86400 сек = 1 сут


