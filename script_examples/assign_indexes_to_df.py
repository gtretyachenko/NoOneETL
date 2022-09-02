# -*- coding: utf-8 -*-

# Генератор индексов (модель)

class IndexExcel:
    # инкапсуляция: __
    # специальный мктод __method__
    __num_count = 7
    tables_property = dict()
    obj_model = dict()
    last_inserted = None

    # конструктор класса
    def __init__(self, tb_name, source=None):
        IndexExcel.tables_property[tb_name][1] += 1
        self.tb_name = tb_name
        self.value = str(IndexExcel.tables_property[tb_name][0]) + str(IndexExcel.tables_property[tb_name][1]).zfill(
            IndexExcel.__num_count)
        self.source = source
        IndexExcel.obj_model[self.value] = self
        IndexExcel.last_inserted = self

    @staticmethod
    def tables():
        return IndexExcel.tables_property.keys()

    @staticmethod
    def count(dict=None):
        res = IndexExcel.obj_model
        if dict:
            res = dict
        return len(res)

# Генератор индексов (реализация)

def index_temp(n, tb_name, df=None):
    for i in range(n):
        yield IndexExcel(tb_name=tb_name, source=df[i])


def index_gen(temp):
    for i, obj_in_ex in enumerate(temp):
        print('>>>Создан объект Класса IndexExcel')
        print(obj_in_ex)
        obj_in_ex.source.append(obj_in_ex.value)
        print(f'Индекс: {obj_in_ex.value}, имя таблицы {obj_in_ex.tb_name}, источник: {obj_in_ex.source}')


IndexExcel.tables_property = {'documents_dkz': ['ddk', 20], 'events': ['eve', 30], 'customers': ['ctm', 40]}

# Имитация датафреймов
df_documents_dkz = [[f'строка {x}'] for x in range(2)]
df_events = [[f'строка {x}'] for x in range(3)]
df_customers = [[f'строка {x}'] for x in range(5)]


# Генерация индексов по таблицам
documents_dkz = index_temp(2, tb_name='documents_dkz', df=df_documents_dkz)
print(documents_dkz)
print('>>>Генератор индексов создан')

print('\n<<<Запуск генерации')
index_gen(temp=documents_dkz)

events = index_temp(3, tb_name='events', df=df_events)
print('\n', events)
print('>>>Генератор индексов создан\n')

print('>>>Запуск генерации')
index_gen(temp=events)

customers = index_temp(3, tb_name='customers', df=df_customers)
print('\n', customers)
print('>>>Генератор индексов создан\n')

print('>>>Запуск генерации')
index_gen(temp=customers)

print('\n>>>Распечатка славаря объектов класса[ключь/объект]: ')
print(*IndexExcel.obj_model.items())

print('\n>>>Распечатка регистратора таблиц и индексов (таблица, [префикс, счетчик]): ')
print(*IndexExcel.tables_property.items())

print('\n>>>Таблицы: ')
print(*IndexExcel.tables(), sep=', ')
# print('всего таблиц:',IndexExcel.tables().count())
print('всего: ', IndexExcel.count(IndexExcel.tables()))

print('\n>>>Индексы: ')
print(*IndexExcel.obj_model, sep=', ')
# print('всего таблиц:',IndexExcel.tables().count())
print('всего: ', IndexExcel.count())
print('\nпредыдущий индекс: ', IndexExcel.last_inserted.value)
