import uvicorn
from fastapi import FastAPI, HTTPException, Body
from fastapi.middleware.cors import CORSMiddleware
from src.italy_collection.data_collection import run_sensor_pipeline

from src.config import logger, public_or_local



if public_or_local == 'LOCAL':
    url = 'http://localhost'
else:
    url = 'http://11.11.11.11'

origins = [
    url
]

docs_url = "/data_collection"

app = FastAPI(docs_url=docs_url, openapi_url=f'{docs_url}/openapi.json')
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.post(f"{docs_url}/italy_data_collection_pipeline")
async def run_pipeline():
    """
    Запускает полный ETL-процесс для получения и загрузки данных с итальянских датчиков.

    Последовательно выполняет следующие шаги:
    1. Получение последней даты из таблицы `load_consumption` в базе данных.
    2. Скачивание новых данных с внешнего API, начиная с этой даты и до текущего момента.
    3. Предобработка данных: фильтрация только новых записей.
    4. Загрузка новых данных в базу данных с обработкой конфликтов по дате (`UPSERT`).

    Возвращает:
        str: Сообщение об успешной загрузке данных или отсутствии новых данных.

    Исключения:
        Поднимает исключения, если возникает ошибка на любом этапе пайплайна.

    Пример вызова Python:

        import requests

        url = "http://your-api-host/docs_url/run_sensor_pipeline"  # замените на реальный URL ручки

        try:
            response = requests.post(url)
            response.raise_for_status()
            data = response.json()
            print("Результат:", data.get("message"))
        except requests.HTTPError as http_err:
            print(f"HTTP ошибка: {http_err}")
        except Exception as err:
            print(f"Ошибка: {err}")
    """
    try:
        result = run_sensor_pipeline()
        print(result)
        return {"message": result}
    except Exception as e:
        import traceback
        error_trace = traceback.format_exc()
        logger.error(error_trace)
        raise HTTPException(
            status_code=500,
            detail=f"Pipeline execution failed: {str(e)}",
            headers={"X-Error": str(e)},
        )



@app.get("/")
def read_root():
    return {"message": "Welcome to the indicators System API"}


if __name__ == "__main__":
    port = 7077
    print(f'🚀 Документация http://0.0.0.0:{port}{docs_url}')
    uvicorn.run("server:app", host="0.0.0.0", port=port, workers=4, log_level="debug")
