import os
import config

import httplib2
from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials

from pycbrf import ExchangeRates

import psycopg2
from psycopg2 import Error
from datetime import *

import telebot
import schedule


def service():
    def get_service_sacc():
        '''Авторизация и получение доступа к таблице'''
        creds_json = os.path.dirname(__file__) + config.json_keys
        scopes = ['https://www.googleapis.com/auth/spreadsheets']
        creds_service =ServiceAccountCredentials.from_json_keyfile_name(creds_json, scopes).authorize(httplib2.Http())
        return build('sheets', 'v4', http=creds_service)


    def  get_data_sheets():
        '''Получение данных с таблицы в resp'''
        sheet = service.spreadsheets()
        sheet_id = config.sheet_id
        return sheet.values().get(spreadsheetId=sheet_id, range=config.range).execute()


    def get_cbrf():
        '''Получение курса доллара'''
        rates = ExchangeRates(date.today())
        return rates['USD'].value


    service = get_service_sacc()
    resp = get_data_sheets()
    usd = get_cbrf()


    '''Получение списка id с Google Sheets'''
    list_id_google = []
    for item in resp['values']:
        if item:
            list_id_google += [item[0]]


    '''Добавление, обновление, удаление записей'''
    try:
        '''Добавление новых записей и обновление старых'''
        connection = psycopg2.connect(user=config.user,
                                      password=config.password,
                                      host=config.host,
                                      port=config.port,
                                      database='postgres')
        cursor = connection.cursor()
        insert_query = """ INSERT INTO channel_service (id, order_number, price_usd, date_post, price_rub) VALUES (%s, %s, %s, %s, %s) 
        ON CONFLICT (id) DO UPDATE SET order_number=EXCLUDED.order_number, price_usd=EXCLUDED.price_usd, date_post=EXCLUDED.date_post, price_rub=EXCLUDED.price_rub"""

        cursor.execute("SELECT id from channel_service")
        record = cursor.fetchall()
        for item in resp['values']:
            if item:
                item_purchase_time = datetime.strptime(item[3], "%d.%m.%Y")
                item_tuple = (item[0], item[1], item[2], item_purchase_time, int(item[2]) * usd)
                cursor.execute(insert_query, item_tuple)
                connection.commit()
                count = cursor.rowcount
                print(count, "Запись успешно добавлена/обновлена")

        '''Получение списка id с БД в множество для сверки удаленных записей'''
        list_id_bd = set()
        for item in record:
            for ids in item:
                list_id_bd.add(ids)

        '''Удаление записей'''
        for item in list_id_bd:
            if str(item) not in list_id_google:
                delete_query = """Delete from channel_service where id = %s"""
                delete_id = (item,)
                print(delete_id)
                cursor.execute(delete_query, delete_id)
                connection.commit()
                count = cursor.rowcount
                print(count, "Запись успешно удалена")

        '''Проверка даты отправки и оповещение в телеграмм'''
        cursor.execute("SELECT order_number,date_post, notification, id  from channel_service")
        check = cursor.fetchall()
        now = date.today()
        for item in check:
            if item[1] == now and item[2] == False:
                update_query = """Update channel_service set notification = TRUE where id = %s"""
                id_notif = (item[3],)
                cursor.execute(update_query, id_notif)
                connection.commit()
                number_id = item[0]
                token = config.token
                bot = telebot.TeleBot(token)
                chat_id = config.chat_id
                text = 'Привет, заказ №' + str(number_id) + ' должен быть поставлен сегодня'
                bot.send_message(chat_id, text)

    except (Exception, Error) as error:
        print("Ошибка при работе с PostgreSQL", error)
    finally:
        if connection:
            cursor.close()
            connection.close()
            print("Соединение с PostgreSQL закрыто")


def main():
    schedule.every(config.timer).minutes.do(service)

    while True:
        schedule.run_pending()

if __name__ == '__main__':
    main()