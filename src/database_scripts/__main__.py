from tinkoff.invest import Client, CandleInterval
from .tinkoff_actions import TINKOFF_TOKEN, get_figi_from_file, get_cached_candles_data, \
    get_all_figi_from_tbank


def get_candles_data(days: int = 1, interval: CandleInterval = CandleInterval.CANDLE_INTERVAL_1_MIN,
                     file_name: str = "candles_info_popular_ru_us.csv"):
    """
    days: кол-во дней исторических данных для загрузки. Глубина истории
    CandleInterval: интервал свечей
    file_name: имя файла с отобранными вручную акциями
    """
    with Client(TINKOFF_TOKEN) as client:
        # ф-и для получения всех доступных акций
        # тк потом обработка была вручную, эти ф-и скрыты. не удалять при первом просмотре!
        # shares_connection = client.instruments.shares()
        # candle_date = get_all_figi_from_tbank(shares_connection)

        figi_list = get_figi_from_file(file_name)

        candles = get_cached_candles_data(
            figi_list,
            days=days,
            interval=interval
        )

        return candles


if __name__ == '__main__':
    candles_data = get_candles_data()
