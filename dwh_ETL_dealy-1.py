# !/usr/bin/python3
# -*- coding: utf-8 -*-


# Импорт системных библиотек
import os
import sys
from datetime import datetime
# Импорт почтового сервера
import smtplib
# Импорт библиотек конструктора электронных писем
from email import encoders
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.header import Header
# Импорт билбиотеки для чтения ini конфигов
from configparser import ConfigParser
# Ипорт библиотек подключения к mysql серверу
import pymysql
from pymysql import Error
# Ипорт аналитических библиотек
import pandas as pd
import numpy as np

# *********************************************************************************************************************
#                                               |Email|                                                               #
# *********************************************************************************************************************
def send_email(cfg, df=None, **email_kwarg):
    # _получение ключей из конфига для рассылки
    _from = cfg.get("smtp", 'from')
    host = cfg.get("smtp", "server")
    pas = cfg.get("smtp", "pass")
    # _создание части письма с типом МультиПарт/Реалэйт (для основного контейнера - письмо)
    msg = MIMEMultipart('related')
    # _создание ключевых заголовков для основного контейнера (письмо)
    msg.add_header('Subject', 'ETL_DWH_NOONE: {subject}.'.format(subject=email_kwarg.get('subject')))
    msg['From'] = _from
    msg['To'] = email_kwarg.get('to_address')
    msg['Cc'] = email_kwarg.get('to_copy')
    # _создание части письма с типом МультиПарт/Альтернатив для вложения в основной контейнер (в письмо)
    msg_part_alternative = MIMEMultipart('alternative')
    msg.attach(msg_part_alternative)
    # _подготовка тела письма в html
    html_data = ''
    html_body = """
        <html>   
          <head></head>
          <body>
                <p>{msg}</p>
                <br>
    """.format(msg=email_kwarg.get('msg'))
    if df:
        for row in df:
            for val in row:
                html_data += (f'<tr><td>{val}</td>' if html_data == '' else f'<td>{val}</td>')
        html_data += f'</tr>'
        html_body = """
                    <table border="1">
                        <tr>
        """
        for i, header in enumerate(df[0]):
            html_body += f'<th>{header}{i}</th>'
        html_body += """
                        </tr>
                        {data_table}
                    </table>
                    <br>
        """.format(data_table=html_data)
    else:
        """         <img src="cid:{cid}"/>
                    <p><tt><b>Автоматизация обменов</b></tt></p>               
                /body>
            </html>
        """.format(cid='image1')
    # _создание части письма для вложения в часть письма MIMEMultipart('alternative') вложенную в осн. контейнер письма
    msg_part_text_html = MIMEText(html_body, 'html')
    msg_part_alternative.attach(msg_part_text_html)

    # _чтение фала в часть письма (image) для вывода в тело письма через html разметку
    with open('logo.png', 'rb') as fp:
        msg_image = MIMEImage(fp.read())

    # _создание ключевого заголовка для части письма (image), вложение части в основной контейнер (письмо)
    msg_image.add_header('Content-ID', '<image1>')
    msg.attach(msg_image)
    count_id = 0
    for path_to_file in email_kwarg.get('path_to_attach_files'):
        count_id += 1
        # _чтение фала в часть письма (Base) для размещения во вложениии письма (добавление в основной контейнер)
        with open(path_to_file, 'rb') as fp:
            attach_file = MIMEBase(
                email_kwarg.get('attach_types')[count_id][0],
                email_kwarg.get('attach_types')[count_id][1],
                filename=os.path.basename(path_to_file)
            )
            # _создание заголовка через класс заголовки для его декодирования с послед. указанием размещения в письме
            h = Header(os.path.basename(path_to_file), 'utf-8').encode()
            attach_file.add_header('Content-Disposition', 'attachment', filename=h)
            # _создание заголовков для индексации вложения в письмо
            attach_file.add_header('X-Attachment-Id', f'{id}')
            attach_file.add_header('Content-ID', f'<{id}>')
            # _загрузка файла в контейнер - часть письма (Base)
            attach_file.set_payload(fp.read())
            # _декодирование и добовление файла
            encoders.encode_base64(attach_file)
            msg.attach(attach_file)
    # Создание экземпляра почтового сервера и отправка письма
    server = smtplib.SMTP(host)
    server.starttls()
    server.login(msg['From'], pas)
    server.send_message(msg)
    server.quit()
    print('Отправлено!')

# *********************************************************************************************************************
#                                               |SQL|                                                                 #
# *********************************************************************************************************************
def create_connection(cfg):
    try:
        conn = pymysql.connect(
            host=cfg.get("mysql", "host"),
            user=cfg.get("mysql", "user"),
            passwd=cfg.get("mysql", "pass"),
            database=cfg.get("mysql", "db")
        )
        print('Connection to MySQL DB successful')
    except Error as e:
        print(f'The error {e} occurred')
        conn = e
    return conn


def executemany_query(conn, query, val):
    cursor = conn.cursor()
    try:
        cursor.executemany(query, val)
        conn.commit()
        res = "Query executed successfully"
        print(res)
    except Error as e:
        res = f"The error '{e}' occurred"
        print(res)
    return res


def execute_query(conn, query):
    cursor = conn.cursor()
    try:
        cursor.execute(query)
        conn.commit()
        res = "Query executed successfully"
        print(res)
    except Error as e:
        res = f"The error '{e}' occurred"
        print(res)
    return res


def execute_read_query(conn, query):
    cursor = conn.cursor()
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except Error as e:
        print(f"The error '{e}' occurred")
        return e

# *********************************************************************************************************************
#                                               |DATA|                                                                #
# *********************************************************************************************************************
def prepare_attr_table(temp_txt):
    headers = ''
    for line in temp_txt:
        headers += ('' if headers == '' else ',') + line
        headers = headers.replace('\n', '')
    variables = '%s,' * (headers.count(',') + 1)
    return headers, variables[:-1]


def prepare_load_data(data):
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
                    # value = value.replace('-', '')
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

    if isinstance(load_data, pd.DataFrame):
        val = prepare_load_data(load_data)
        sql = ''
        if method == 'REP':
            sql = f'REPLACE INTO {name_table} ({str_header}) VALUES ({str_variables})'
        elif method == 'SEL-REP':
            # sql = f'SELECT {str_header} FROM {name_table}'
            # res = executemany_query(conn, sql)
            pass
            sql = f'REPLACE INTO {name_table} ({str_header}) VALUES ({str_variables})'
        elif method == 'INS':
            sql = f'INSERT INTO {name_table} ({str_header}) VALUES ({str_variables})'
        elif method == 'DEL-INS':
            sql = f'TRUNCATE TABLE {name_table}'
            execute_read_query(conn, sql)
            sql = f'INSERT INTO {name_table} ({str_header}) VALUES ({str_variables});'
        res = executemany_query(conn, sql, val)
    return res


# *********************************************************************************************************************
#                                               |Start procedure|                                                    #
# *********************************************************************************************************************
base_path = os.path.dirname(os.path.abspath(__file__))
config_path = os.path.join(base_path, "config.ini")
if os.path.exists(config_path):
    config = ConfigParser()
    config.read(config_path)
else:
    print('Config not found! Exiting!')
    sys.exit(1)
to_address = ['g.tretyachenko@noone.ru']
connection = create_connection(config)
if isinstance(connection, BaseException):
    send_email(
        cfg=config,
        to_address='',
        to_copy='',
        subject='Error connection to MySQL DB',
        msg=f'The error {connection} occurred',
        path_to_attach_files=[]
    )
    sys.exit(1)
names_table = [
    ['info_current_price', 'DEL-INS'],
    ['fact_sales_extendet_dealy', 'INS'],
    ['rep_orders_ecom_at_work', 'DEL-INS'],
    ['rep_control_shopping_rooms', 'DEL-INS'],
    ['fact_current_stock_extendet', 'DEL-INS'],
    ['info_current_season_products', 'DEL-INS'],
    ['fact_current_accum_by_seasons', 'DEL-INS']
]
str_tables = ''
text_msg = ''
for twin in names_table:
    if twin[1] != 'skip':
        text_msg = load_data_to_dwh(connection, twin[0], twin[1])
    str_tables += ('"' + twin[0] + '"' if str_tables == '' else ',"' + twin[0] + '"')

sql_script = 'CALL update_tables_control_dealy()'
update_sys = execute_query(connection, sql_script)
sql_script = f'SELECT name_table, updated_date, updated_by FROM sys_update WHERE name_table IN ({str_tables})'
table_update = execute_read_query(connection, sql_script)
send_email(
    cfg=config,
    df=table_update,
    to_address='',
    to_copy='',
    subject='',
    msg=text_msg,
    path_to_attach_files=[os.path.join(base_path, 'logs_file.txt')]
)
