При помощи скрипта на Python сгенерируем DataFrame с синтетическими данными на 10000 строк.

Информация о колонках:
- 1-я колонка – user_id – идентификатор пользователя. Длина user_id должна равняться 15-ти символам. Идентификатор состоит из случайной комбинации следующих символов:
"1234567890abcdefghijk". Для каждой строки в DataFrame значение user_id формируются
случайным образом.
- 2-я колонка – order_number – номер заказа. Столбец необходимо заполнить случайными
значениями в диапазоне от 1 до 10.
- 3-я колонка – click2delivery – время, прошедшее с момента оформления заказа до вручения
клиенту. Столбец необходимо заполнить случайными значениями из нормального распределения
со средним 1440 и стандартным отклонением 200.
- 4-я колонка – order_items_sum – общая стоимость заказа. Значения для этого столбца необходимо
взять из экспоненциального распределения с параметром λ = 1, смещённого на +1.
- 5-я колонка – retention – день жизни покупателя, в который он совершил заказ. Необходимо
сгенерировать значения 1, 2, 3, 4, 5 с вероятностями 0.35, 0.25, 0.2, 0.15 и 0.05 соответственно.
- 6-я колонка - average_time - среднее значение времени доставки по группе. Сгруппированно по номеру заказа для всех строк исходного датасета.
- 7-я колонка - sequence - колонка со значениями последовательности, начинающаяся с [0, 1], где каждый следующий элемент является суммой двух предыдуших, умноженных на 0.5.
- 8-я колонка - user_id_2 - колонка, в которой все строки получены из строк user_id  следующим образом: все буквы в той последовательности, в которой они встречаются в user_id, затем квадрат
числа, полученного из всех цифр в user_id в той последовательности, в которой они встречаются в
user_id.
