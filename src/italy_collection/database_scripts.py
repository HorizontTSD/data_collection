from src.clients.create_clients import get_db_connection



def create_timescale_tables(tables_mapping):
    col_time = 'datetime'
    create_base_sql = """
        CREATE TABLE IF NOT EXISTS {table_name} (
            {col_time} TIMESTAMPTZ NOT NULL,
            {col_name} DOUBLE PRECISION
        );

        -- Создание гипертаблицы
        SELECT create_hypertable('{table_name}', '{col_time}', if_not_exists => TRUE);

        -- Создание уникального индекса
        CREATE UNIQUE INDEX IF NOT EXISTS unique_{table_name}_{col_time}
        ON {table_name} ({col_time});
    """

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                for table in tables_mapping:
                    table_name = table["table_name"]
                    col_name = table["target_col_name"]
                    sql = create_base_sql.format(
                        table_name=table_name,
                        col_time=col_time,
                        col_name=col_name
                    )
                    cur.execute(sql)
                conn.commit()
        print(f"Таблицы {tables_mapping} успешно созданы.")
    except Exception as e:
        print(f"Ошибка при создании таблиц: {e}")


def drop_timescale_tables(to_drop_table_names):
    drop_sql = """
        DROP TABLE IF EXISTS {table_name} CASCADE;
    """

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                for table in to_drop_table_names:
                    sql = drop_sql.format(table_name=table)
                    cur.execute(sql)
                conn.commit()
        print(f"Таблицы {to_drop_table_names} успешно удалены.")
    except Exception as e:
        print(f"Ошибка при удалении таблиц: {e}")

if __name__ == "__main__":
    # to_drop_table_names = ['xgb_predict_load_consumption']
    # drop_timescale_tables(to_drop_table_names)

    tables_mapping = [{"table_name": "xgb_predict_load_consumption", "target_col_name": "load_consumption"}]

    create_timescale_tables(tables_mapping)
