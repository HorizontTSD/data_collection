import time
from src.database_scripts.tinkoff_actions import get_figi_from_file, get_lonely_figi_data, \
    fetch_and_write_data_from_db, process_all_stocks_multithreaded, safe_print, check_table_exists
from transliterate import translit

SIZE_PACKAGE_DAYS = 4


if __name__ == '__main__':
    figi_list, company_list = get_figi_from_file('candles_info_popular_ru_us.csv')

    while True:
        # Создаем конфигурацию для каждой акции вида имя_файла, размер пакета
        stocks_config = []
        company_eng_list = []
        for i in range(len(company_list)):
            company_eng_name = translit(company_list[i], 'ru', reversed=True)
            company_eng_name = company_eng_name.replace(' ', '_')
            company_eng_name = company_eng_name.replace("'", '')
            company_eng_name = company_eng_name.replace("_&", '')
            company_eng_name = company_eng_name.replace("’", '')
            company_eng_name = company_eng_name.replace(".", '')
            company_eng_name = company_eng_name.replace("-", '_')

            company_eng_list.append(company_eng_name)

        for i, figi in enumerate(figi_list):


            table_name = f'{figi + "_" + company_eng_list[i]}'
            stocks_config.append((table_name, SIZE_PACKAGE_DAYS))  # (table_name, days, figi)

        process_all_stocks_multithreaded(stocks_config, max_workers=3)
        time.sleep(10) # 86400 сек = 1 сут


