# !/usr/bin/python3
# -*- coding: utf-8 -*-

# Импорт системных библиотек
import os
import sys
# from datetime import datetime
# Импорт почтового сервера
# import smtplib
# Импорт библиотек конструктора электронных писем
# import email.message
# Импорт билбиотеки для чтения ini конфигов
from configparser import ConfigParser
# Ипорт библиотек подключения к mysql серверу
import pymysql
from pymysql import Error
# Ипорт аналитических библиотек
import pandas as pd
# import numpy as np


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
        print('Processing....')
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
        print('Processing....')
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
    try:
        print('Processing....')
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except Error as e:
        print(f"The error '{e}' occurred")

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

if connection:
      sql = '''
        SELECT DISTINCT 
            Date, 
            COUNT(SdID), 
            SUM(Quantity) 
        FROM fact_traffic_shopping_centers
        GROUP BY Date
        ORDER BY Date
        '''
      query_result = execute_read_query(connection, sql)
      df = pd.DataFrame(query_result)
      df.to_csv('tmp.csv', encoding="utf-8-sig")
