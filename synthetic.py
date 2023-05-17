import pandas as pd
import numpy as np
import psycopg2
import random
import string


def generate_user_id():
    '''определяем функцию для генерации случайной строки из 15 символов'''
    return ''.join(random.choices(string.ascii_letters+string.digits, k=15))

# задаем среднее и стандартное отклонение для нормального распределения
mean, std = 1440, 200

# сгенерируем массив случайных значений из экспоненциального распределения
lam, shift = 1, 1
order_items_sum = np.random.exponential(scale=1/lam, size=10000) + shift

# задаем вероятности для каждого значения retention
prob = [0.35, 0.25, 0.2, 0.15, 0.05]
# генерируем массив значений retention с заданными вероятностями
retention = np.random.choice([1, 2, 3, 4, 5], size=10000, p=prob)


# генерируем датасет с 10000 строками и 5 колонками
synthetic = pd.DataFrame({'user_id': [generate_user_id() for i in range(10000)],
                     'order_number': [random.randrange(1, 11) for i in range(10000)],
                     'click2delivery': np.random.normal(mean, std, size=10000),
                     'order_items_sum': order_items_sum,
                     'retention': retention})

# найдем среднее время доставки для каждого номера заказа
group_orders = synthetic.groupby('order_number', as_index=False).agg({'click2delivery': 'mean'}).rename(columns={'click2delivery': 'average_time'})

# соединим новый столбец (среднее время) с нашим основным датафреймом
synthetic = synthetic.merge(group_orders, on='order_number')

# создадим колонку со значениями последовательности, начинающейся с [0, 1],
# где каждый следующий элемент является суммой двух предыдуших, умноженных на 0.5
seq = [0, 1]
for i in range(2, 10000):
    num = 0.5 * (seq[i-1] + seq[i-2])
    seq.append(num)

synthetic['sequence'] = pd.Series(seq)

def func_for_user_id(x):
    '''функция, которая принимает на вход значение user_id и возвращает строку следующего вида:
    все буквы в той последовательности, в которой они встречаются в user_id, затем квадрат числа,
    полученного из всех цифр в user_id в той последовательности, в которой они встречаются в user_id'''
    lst = list(x)
    digit, letter = [], []
    for l in lst:
        if l.isdigit():
            digit.append(l)
        elif l.isalpha():
            letter.append(l)
    le = ''.join(letter)
    if len(digit) == 0:
        return le
    else:
        di = str(int(''.join(digit)) ** 2)
        return le + di

# применем функцию func_for_user_id к столбцу user_id и создадим на основе нее новый столбик user_id_2
synthetic['user_id_2'] = synthetic['user_id'].apply(func_for_user_id)

synthetic['click2delivery'] = synthetic['click2delivery'].round(3)
synthetic['order_items_sum'] = synthetic['order_items_sum'].round(3)
synthetic['average_time'] = synthetic['average_time'].round(3)
synthetic['sequence'] = synthetic['sequence'].round(3)


# теперь загрузим полученный датафрейм df в локальную базу данных test_db

db_params = {
        'host': 'localhost',
        'database': 'test_db',
        'user': 'postgres',
        'password': 'password',
        'port': '5432'
}

with psycopg2.connect(**db_params) as connection:
    connection.autocommit = True
    with connection.cursor() as cursor:
            cursor.execute('DROP TABLE IF EXISTS synthetic')

            create_table = ''' CREATE TABLE IF NOT EXISTS synthetic (
                                        user_id  varchar(20),
                                        order_date int,
                                        click2delivery numeric(8, 3),
                                        order_items_sum numeric(5, 3),
                                        retention int,
                                        average_time numeric(8, 3),
                                        sequence numeric(8, 6),
                                        user_id_2 varchar(60)
                                        )
                                        '''
            cursor.execute(create_table)

            for row in synthetic.itertuples(index=False):
                cursor.execute("""INSERT INTO synthetic (user_id, 
                                                        order_date, 
                                                        click2delivery, 
                                                        order_items_sum,
                                                        retention, 
                                                        average_time, 
                                                        sequence, 
                                                        user_id_2) 
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
                               row)

            # получим первые 10 строк сгенерированной таблицы
            cursor.execute('SELECT * FROM synthetic LIMIT 10')
            rows = cursor.fetchall()

            for row in rows:
                print(*row)
