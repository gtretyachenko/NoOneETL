import csv

# name.csv ниже нужно заменить на название csv файла, который вы хотите открыть

file_path = r'D:\Курс Data Scientist (SkillBox)\Data Scientist. Аналитика. Начальный уровень\7.1 Знакомство с' \
            r' задачей, понятие «конверсии»\click_stream2.csv'

with open(file_path, mode='r') as csv_file:  # открываем файл
    csv_reader = csv.DictReader(csv_file, fieldnames=['ID', 'page', 'date', 'core'])
    # csv_reader = csv.reader(csv_file, delimiter=str, quotechar=str)

    for row in csv_reader:
        pass

