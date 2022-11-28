# !/usr/bin/python3
# -*- coding: utf-8 -*-

# Импорт системных библиотек
import os
import sys
from datetime import datetime
# Импорт почтового сервера
import smtplib
# Импорт библиотек конструктора электронных писем
import email.message
# Импорт билбиотеки для чтения ini конфигов
from configparser import ConfigParser
# Ипорт библиотек подключения к mysql серверу
import pymysql
from pymysql import Error
# Ипорт аналитических библиотек
import pandas as pd
import numpy as np



def send_email(subject, to_addr, msg_txt, cfg, data_frame=None):

    host = cfg.get("smtp", "server")
    pas = cfg.get("smtp", "pass")
    from_addr = cfg.get("smtp", 'from')
    html = ''
    result = ''
    if data_frame:
        for ii in data_frame:
            x = 0
            for i in ii:
                result += (f'<tr><td>{i}</td>' if x == 0 else f'<td>{i}</td>')
                x += 1
        result += f'</tr>'

        html = f"""\
        <html>   
          <head></head>
          <body>
                <tt>{msg_txt}</tt>
                <br>
                <table border="1">
                    <tr>
                        <th>Table</th>
                        <th>Date</th>
                        <th>User</th>
                    </tr>
                    {result}
                </table>
          </body>
        </html>
        """

    msg = email.message.Message()
    msg['Subject'] = subject
    msg['From'] = from_addr
    msg['To'] = to_addr
    password = pas

    msg.add_header('Content-Type', 'text/html')
    msg.set_payload(html)
    server = smtplib.SMTP(host)
    server.starttls()

    server.login(msg['From'], password)
    server.sendmail(msg['From'], [msg['To']], msg.as_string())
    server.quit()


def create_connection(cfg):
    connection = None
    try:
        connection = pymysql.connect(
            host=cfg.get("mysql", "host"),
            user=cfg.get("mysql", "user"),
            passwd=cfg.get("mysql", "pass"),
            database=cfg.get("mysql", "db")
        )
        print('Connection to MySQL DB successful')
    except Error as e:
        print(f'The error {e} occurred')
    return connection


def executemany_query(connection, query, val):
    cursor = connection.cursor()
    try:
        cursor.executemany(query, val)
        connection.commit()
        res = "Query executed successfully"
        print(res)
    except Error as e:
        res = f"The error '{e}' occurred"
        print(res)
    return res


def execute_query(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        res = "Query executed successfully"
        print(res)
    except Error as e:
        res = f"The error '{e}' occurred"
        print(res)
    return res


def execute_read_query(connection, query):
    cursor = connection.cursor()
    result = None
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except Error as e:
        print(f"The error '{e}' occurred")


def prepare_attr_table(temp_txt):
    headers = ''
    for line in temp_txt:
        headers += ('' if headers == '' else ',') + line
        headers = headers.replace('\n', '')
    variables = '%s,' * (headers.count(',') + 1)
    return headers, variables[:-1]


def prepare_load_data(data, table_name):
    new_data = list()
    data = data.replace(np.NAN, None)
    for i_row, values in enumerate(data.values):
        # Перебрать строки в таблице c заполнением нового столбца
        item_data = list()
        for i_col, value in enumerate(values):
            # Перебрать столбцы в строке
            if isinstance(value, str):
                # Если текст
                temp_val = value.replace(' 0:00:00', '').replace(' 0:00', '')
                if temp_val != value and len(temp_val) == 10:
                    # Формат ДатаВремя 1С распознан и сконвертирован
                    value = datetime.strptime(temp_val, '%d.%m.%Y')
                    value = value.strftime('%Y-%m-%d')
                else:
                    # Создаем текст (строку) в буфере
                    temp_val = value.replace('.', '').replace(',', '').replace(chr(160), '').replace(chr(32), '').replace('-', '')
                if temp_val.isnumeric():
                    # Проверяем текст (строку) в буфере на цифры
                    if value.replace(',', '.').replace(chr(160), '').replace(chr(32), '') != temp_val and not value.isnumeric():
                        value = value.replace(',', '.').replace(chr(160), '').replace(chr(32), '')
                        if (value[0] == '-' and value.replace('-', '') != temp_val) or value.replace('-', '') != temp_val:
                            # Если только числа и была запятая, то конвертируем во float
                            value = float(value.replace(',', '.').replace(chr(160), '').replace(chr(32), ''))
                        else:
                            value = value.replace(',', '.').replace(chr(160), '').replace(chr(32), '')

            ### Выполнить если значение текст (строка)
            if table_name == 'fact_sales_extendet_dealy_temp':
                ### Выполнить если работаем с таблицой расширеных продаж
                if i_col == 25 and isinstance(value, str):
                        value = value.replace(chr(160), '').replace(chr(32), '').replace('БК', '').replace('', '')
                if i_col == 26 and data[26].values[i_row] and isinstance(value, str):
                    value = value[:value.find(' ')]
                    if len(value) == 8:
                        value = value[2:]
                if i_col == 27 and isinstance(value, str):
                    temp_val = value.replace('Клиент, код карты: ', '').replace(chr(160), '').replace(chr(32), '')
                    if value != temp_val and temp_val.isnumeric() and len(temp_val) == 6:
                        data[25].values[i_row] = temp_val
                        data[27].values[i_row] = 'Розничный покупатель'

            if not value:
                ### Если Пусто
                item_data.append(None)
            else:
                if isinstance(value, float):
                    ###  когда тип с плавающей точкой (flot)
                    value = round(value, 3)
                    value = str(value) # .replace('.', ',')
                ### Если Любое
                item_data.append(value)
        if table_name == 'fact_sales_extendet_dealy_temp':
            ### выполнить если таблица расширеных продаж
            item_data.append(None)
            if item_data[25] and isinstance(item_data[25], str):
                code = str(item_data[25]).replace(chr(160), '').replace(chr(32), '').replace('БК', '')
                if code.isnumeric() and len(code) == 6:
                    # Код БК vВАЛИДНЫЙv
                    item_data[28] = 'True'
                else:
                    # Код БК хНЕ ВАЛИДНЫЙх
                    item_data[28] = 'False'
        new_data.append(tuple(item_data))
    return new_data


def load_data_to_dwh(conn, name_table, method, load_data=None):
    dt_start_day = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
    temp_txt = open(f'C:/Users/g.tretyachenko/PycharmProjects/NoOneETL/{name_table}_headers.txt', 'r')
    # Получение заголовков столбцов таблицы
    str_header, str_variables = prepare_attr_table(temp_txt)
    res = ''
    # Мапинг дирикторий файлов csv & table_name
    if name_table[:3] == 'rep':
        if os.path.getmtime(f'C:/Общая/_DWH/_Reports/{name_table}.csv') > dt_start_day:
            load_data = pd.read_csv(f'C:/Общая/_DWH/_Reports/{name_table}.csv', header=None, skiprows=[0], sep=';',
                                    dtype=str)
    elif name_table[:3] == 'dim':
        if os.path.getmtime(f'C:/Общая/_DWH/_Dimension/{name_table}.csv') > dt_start_day:
            load_data = pd.read_csv(f'C:/Общая/_DWH/_Dimension/{name_table}.csv', header=None, skiprows=[0], sep=';',
                                    dtype=str)
    elif name_table[:3] == 'inf':
        if os.path.getmtime(f'C:/Общая/_DWH/_Info/{name_table}.csv') > dt_start_day:
            load_data = pd.read_csv(f'C:/Общая/_DWH/_Info/{name_table}.csv', header=None, skiprows=[0], sep=';',
                                    dtype=str)
    # elif name_table[:4] == 'stat':
    #     if os.path.getmtime(f'C:/Общая/_DWH/_Stat/{name_table}.csv') > dt_start_day:
    #         load_data = pd.read_csv(f'C:/Общая/_DWH/_Stat/{name_table}.csv', header=None, skiprows=[0], sep=';',
    #                                 dtype=str)
    elif name_table == 'fact_sales_extendet_dealy_temp':
        # if os.path.getmtime(f'C:/Общая/_DWH/_Facts_by_periods/Sales/{name_table}.csv') > dt_start_day:
            load_data = pd.read_csv(f'C:/Общая/_DWH/_Facts_by_periods/Sales/{name_table}.csv', header=None,
                                    skiprows=[0], sep=';', dtype=str)
    elif name_table == 'fact_sales_extendet_dealy':
        if os.path.getmtime(f'C:/Общая/_DWH/_Facts_by_periods/Sales/{name_table}.csv') > dt_start_day:
            load_data = pd.read_csv(f'C:/Общая/_DWH/_Facts_by_periods/Sales/{name_table}.csv', header=None,
                                    skiprows=[0], sep=';', dtype=str)
    elif name_table == 'fact_current_stock_extendet':
        if os.path.getmtime(f'C:/Общая/_DWH/_Facts_by_periods/Stock/{name_table}.csv') > dt_start_day:
            load_data = pd.read_csv(f'C:/Общая/_DWH/_Facts_by_periods/Stock/{name_table}.csv', header=None,
                                    skiprows=[0], sep=';', dtype=str)
    elif name_table == 'fact_current_accum_by_seasons':
        if os.path.getmtime(f'C:/Общая/_DWH/_Facts_by_seasons/{name_table}.csv') > dt_start_day:
            load_data = pd.read_csv(f'C:/Общая/_DWH/_Facts_by_seasons/{name_table}.csv', header=None, skiprows=[0],
                                    sep=';', dtype=str)
    elif name_table == 'fact_current_addition_retail_stock_extendet':
        if os.path.getmtime(f'C:/Общая/_DWH/_Facts_by_periods/Stock/{name_table}.csv') > dt_start_day:
            load_data = pd.read_csv(f'C:/Общая/_DWH/_Facts_by_periods/Stock/{name_table}.csv', header=None, skiprows=[0],
                                    sep=';', dtype=str)
    elif name_table == 'fact_current_shepping_list_by_seasons':
        if os.path.getmtime(f'C:/Общая/_DWH/_Facts_by_seasons/{name_table}.csv') > dt_start_day:
            load_data = pd.read_csv(f'C:/Общая/_DWH/_Facts_by_seasons/{name_table}.csv', header=None, skiprows=[0],
                                    sep=';', dtype=str)
    elif name_table == 'fact_current_accum_division_of_seasons_by_tt':
        if os.path.getmtime(f'C:/Общая/_DWH/_Facts_by_seasons/{name_table}.csv') > dt_start_day:
            load_data = pd.read_csv(f'C:/Общая/_DWH/_Facts_by_seasons/{name_table}.csv', header=None, skiprows=[0],
                                    sep=';', dtype=str)

    if isinstance(load_data, pd.DataFrame):
        val = prepare_load_data(load_data, name_table) # список картежей с
        sql = ''
        if method == 'REP':
            sql = f'REPLACE INTO {name_table} ({str_header}) VALUES ({str_variables})'
        elif method == 'SEL-REP':
            pass
            sql = f'REPLACE INTO {name_table} ({str_header}) VALUES ({str_variables})'
        elif method == 'INS':
            sql = f'INSERT INTO {name_table} ({str_header}) VALUES ({str_variables})'
        elif method == 'DEL-INS':
            sql_0 = f'TRUNCATE TABLE {name_table}'
            execute_read_query(conn, sql_0)
            sql = f'INSERT INTO {name_table} ({str_header}) VALUES ({str_variables});'
        res = executemany_query(conn, sql, val)
        if res == 'Query executed successfully':
            df = pd.DataFrame(val)
            df.replace(to_replace=[None], value=np.nan, inplace=True)
            arr = np.array(df)
            arr_headers = np.array(str_header.split(','))
            outfile = os.path.join(base_path, 'eda_df') # TemporaryFile()
            outfile2 = os.path.join(base_path, 'headers(eda_df)')  # TemporaryFile()
            np.save(outfile, arr)
            np.save(outfile2, arr_headers)
    return res


""" 
|Start procedure|
"""
base_path = os.path.dirname(os.path.abspath(__file__))
config_path = os.path.join(base_path, "config.ini")
if os.path.exists(config_path):
    cfg = ConfigParser()
    cfg.read(config_path)
else:
    print("Config not found! Exiting!")
    sys.exit(1)

connection = create_connection(cfg)
msg_txt = ''
if connection:
    str_tables = ''
    names_table = [
       # ['info_current_price', 'DEL-INS'],
       # ['fact_sales_extendet_dealy', 'INS'],
       # ['rep_orders_ecom_at_work', 'DEL-INS'],
       # ['rep_control_shopping_rooms', 'DEL-INS'],
       # ['rep_control_down_stores', 'DEL-INS'],
       # ['fact_current_addition_retail_stock_extendet', 'DEL-INS'],
       # ['fact_current_stock_extendet', 'DEL-INS'],
       # ['info_current_season_products', 'DEL-INS'],
       # ['fact_current_accum_by_seasons', 'DEL-INS'],
       # ['rep_waiting_goods_arrival_warehouse1', 'DEL-INS'],
       # ['fact_current_shepping_list_by_seasons', 'DEL-INS'],
       # ['fact_current_accum_division_of_seasons_by_tt', 'DEL-INS']
        ['fact_sales_extendet_dealy_temp', 'DEL-INS'],
    ]
    for twin in names_table:
        if twin[1] != 'skip':
            msg_txt = load_data_to_dwh(connection, twin[0], twin[1])
        str_tables += ('"' + twin[0] + '"' if str_tables == '' else ',"' + twin[0] + '"')
    # sql = 'CALL update_tables_control_dealy()'
    # update_sys = execute_query(connection, sql)

    sql = f'SELECT name_table, updated_date, updated_by FROM sys_update WHERE name_table IN ({str_tables})'
    info_update = execute_read_query(connection, sql)
    subject = "dwh_ETL"
    to_addr = "g.tretyachenko@noone.ru"
    send_email(subject, to_addr, msg_txt, cfg, info_update)
else:
    msg_txt = 'Error connection to MySQL DB'
    subject = "dwh_ETL: error connection"
    to_addr = "g.tretyachenko@noone.ru"
    send_email(subject, to_addr, msg_txt, cfg)
