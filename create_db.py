import psycopg2
from psycopg2 import Error
import config

try:
    connection = psycopg2.connect(user=config.user,
                                  password=config.password,
                                  host=config.host,
                                  port=config.port,
                                  database=config.database)
    cursor = connection.cursor()
    create_table_query = '''CREATE TABLE channel_service
                          (ID INT PRIMARY KEY      NOT NULL,
                           ORDER_NUMBER    INTEGER NOT NULL,
                           PRICE_USD       MONEY   NOT NULL,
                           DATE_POST       DATE    NOT NULL,
                           PRICE_RUB       MONEY   NOT NULL,
                           NOTIFICATION    BOOLEAN NOT NULL  DEFAULT FALSE); '''
    cursor.execute(create_table_query)
    connection.commit()
    print("Таблица успешно создана в PostgreSQL")

except (Exception, Error) as error:
    print("Ошибка при работе с PostgreSQL", error)
finally:
    if connection:
        cursor.close()
        connection.close()
        print("Соединение с PostgreSQL закрыто")