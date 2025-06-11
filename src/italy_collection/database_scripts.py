from src.clients.create_clients import get_db_connection


table_names = ['load_consumption']
col_time = 'datetime'

def create_timescale_tables(table_names):
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
                for table in table_names:
                    sql = create_base_sql.format(
                        table_name=table,
                        col_time=col_time,
                        col_name=table
                    )
                    cur.execute(sql)
                conn.commit()
        print(f"Таблицы {table_names} успешно созданы.")
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
    to_drop_table_names = ['load_consumption']
    # drop_timescale_tables(to_drop_table_names)
    create_timescale_tables(table_names)
