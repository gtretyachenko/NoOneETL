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
    #

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
    for values in data.values:
        item_data = list()
        for value in values:
            if isinstance(value, str):
                temp_val = value.replace(' 0:00:00', '').replace(' 0:00', '')
                if temp_val != value and len(temp_val) == 10:
                    value = datetime.strptime(temp_val, '%d.%m.%Y')
                    value = value.strftime('%Y-%m-%d')
                temp_val = value.replace(',', '').replace(chr(160), '').replace(chr(32), '').replace('-', '')
                if temp_val.isnumeric():
                    value = value.replace(',', '.')
                    value = value.replace(chr(160), '')
                    value = value.replace(chr(32), '')
                temp_val = value.replace(chr(160), '')
                if temp_val.isnumeric():
                    value = value.replace(chr(160), '')
                temp_val = value.replace(chr(32), '')
                if temp_val.isnumeric():
                    value = value.replace(chr(32), '')
            if type(value) is float:
                value = int(value)
            if not value:
                item_data.append(value)
            else:
                item_data.append(str(value))
        new_data.append(tuple(item_data))
    return new_data


def load_data_to_dwh(conn, name_table, method, load_data=None):
    dt_start_day = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
    temp_txt = open(f'C:/Users/g.tretyachenko/PycharmProjects/NoOneETL/{name_table}_headers.txt', 'r')
    str_header, str_variables = prepare_attr_table(temp_txt)
    res = ''

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
    elif name_table[:4] == 'stat':
        if os.path.getmtime(f'C:/Общая/_DWH/_Stat/{name_table}.csv') > dt_start_day:
            load_data = pd.read_csv(f'C:/Общая/_DWH/_Stat/{name_table}.csv', header=None, skiprows=[0], sep=';',
                                    dtype=str)
    elif name_table == 'fact_sales_extendet_dealy_temp':
        if os.path.getmtime(f'C:/Общая/_DWH/_Facts_by_periods/Sales/{name_table}.csv') > dt_start_day:
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
        val = prepare_load_data(load_data, name_table)
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
       ['info_current_price', 'DEL-INS'],
       ['fact_sales_extendet_dealy', 'INS'],
       ['rep_orders_ecom_at_work', 'DEL-INS'],
       ['rep_control_shopping_rooms', 'DEL-INS'],
       ['rep_control_down_stores', 'DEL-INS'],
       ['fact_current_addition_retail_stock_extendet', 'DEL-INS'],
       ['fact_current_stock_extendet', 'DEL-INS'],
       ['info_current_season_products', 'DEL-INS'],
       ['fact_current_accum_by_seasons', 'DEL-INS'],
       ['rep_waiting_goods_arrival_warehouse1', 'DEL-INS'],
       ['fact_current_shepping_list_by_seasons', 'DEL-INS'],
       ['fact_current_accum_division_of_seasons_by_tt', 'DEL-INS']
       #  ['fact_sales_extendet_dealy_temp', 'DEL-INS'],
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
