import math


a, b = 2, 2
res, step = 0, -1 if b < 0 else 1
for cnt in range(0, b, step):
    res += a if step > 0 else -a
print('Результат: ', res)
print('Проверка: ', a * b)

cnt, res = 0, 0
while cnt != abs(b):
    res += a if b > 0 else -a
    cnt += 1
print('Результат: ', res)
print('Проверка: ', a * b)


a, b = 3, 1
x = (a - 1 - a % 2) + (a % 2 * 2)
for cnt in range(x, 0, -2):
    print(cnt)


a, b = 17, 164
for cnt in range(math.ceil(math.sqrt(a)), math.ceil(math.sqrt(b))):
    print('sqrt: ', cnt)


a, b = 45, 90
aa, bb = 0, 0
for i in str(a):
    aa += int(i)
for i in str(b):
    bb += int(i)
res = 1 if aa > bb else 2 if aa < bb else 0
print(res)



limit_nums = int(input('Какое кол-во чисел будем проверять?'))
list_nums, i = [], 0
while len(list_nums) < limit_nums:
    list_nums.append(int(input(f'Осталось {limit_nums - i}. Введите число:  ')))
    i += 1


# cnt_zero = list_nums.count(0)
cnt_zero = list(map(lambda x: 1 if x == 0 else 0, list_nums))
print(f'Найдено нолей {cnt_zero}')


new_list = ()

f2 = [['q', 'w', 'e', 'r'], ['q', 'w', 'e', 'r'], ['q', 'w', 'e', 'r'], ['q', 'w', 'e', 'r']]
cnt = len([x for x in f2])








