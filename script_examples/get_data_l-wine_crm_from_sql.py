# !/usr/bin/python3
# -*- coding: utf-8 -*-

import pymysql
from pymysql import Error


def create_connection(host_name, user_name, user_password, db_name):
    connection = None
    try:
        connection = pymysql.connect(
            host=host_name,
            user=user_name,
            passwd=user_password,
            database=db_name
        )
        print('Connection to MySQL DB successful')
    except Error as e:
        print(f'The error {e} occurred')

    return connection


def execute_read_query(connection, query):
    cursor = connection.cursor()
    result = None
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except Error as e:
        print(f"The error '{e}' occurred")


connection = create_connection('185.12.29.142', 'tretyachenko', 'Fah8Ohn7', '49900')

select_data = """
SELECT DISTINCT
  bie.id AS event_id,
  bie.DATE_CREATE AS event_create_date,
  biep2.VALUE AS event_date,
  biep3.VALUE AS event_number_SAD,
  bie.name AS event_name,
  biep4.VALUE AS event_responsible,
  biep.VALUE AS participans_id,
  bcc.id AS contact_id,  
  bcc.DATE_CREATE AS contact_date_create,
  bcc.SOURCE_ID AS contact_source,
  bcc.full_name AS contact_full_name,
  bcd.id AS deal_id,
  bcd.DATE_CREATE AS deal_date,
  bcd.CURRENCY_ID AS deal_currency,
  IFNULL(bcd.OPPORTUNITY,0) AS deal_sum,  
  bcd.STAGE_ID AS deal_status
FROM b_iblock_element AS bie
  LEFT JOIN (SELECT * FROM b_iblock_element_property  AS biep WHERE biep.IBLOCK_PROPERTY_ID = 273) AS biep
  ON (bie.id = biep.IBLOCK_ELEMENT_ID)
  LEFT JOIN (SELECT * FROM b_iblock_element_property  AS biep2 WHERE biep2.IBLOCK_PROPERTY_ID = 270) AS biep2
  ON (bie.id = biep2.IBLOCK_ELEMENT_ID)
  LEFT JOIN (SELECT * FROM b_iblock_element_property  AS biep3 WHERE biep3.IBLOCK_PROPERTY_ID = 271) AS biep3
  ON (bie.id = biep3.IBLOCK_ELEMENT_ID)
  LEFT JOIN (SELECT * FROM b_iblock_element_property  AS biep4 WHERE biep4.IBLOCK_PROPERTY_ID = 458) AS biep4
  ON (bie.id = biep4.IBLOCK_ELEMENT_ID)       
  LEFT JOIN  b_crm_contact AS bcc
  ON (biep.VALUE = bcc.id)
  LEFT JOIN (SELECT * FROM b_crm_deal AS bcd WHERE bcd.STAGE_ID LIKE '%EXECUTING' OR bcd.STAGE_ID LIKE '%WON' ) AS bcd
  ON (bcc.id = bcd.CONTACT_ID AND bcd.DATE_CREATE BETWEEN biep2.VALUE AND DATE_ADD(biep2.VALUE, INTERVAL 180 DAY))
WHERE bie.IBLOCK_ID = 66
ORDER BY event_date, contact_full_name
"""
data_db = execute_read_query(connection, select_data)
print(type(data_db))