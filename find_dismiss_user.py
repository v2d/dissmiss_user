#!/usr/bin/python
# -*- coding: utf-8
import pymysql
import pyodbc
import string
from datetime import datetime, date, time

start_time = datetime.today()

pymysql.install_as_MySQLdb() # включаем совместимость с mysqldb

# Соединяемся с клиентской БД, для поиска уволенных сотрудников
#
driver = 'DRIVER={SQL Server}'
server = 'SERVER=10.100.50.54'
port = 'PORT=1433'
db = 'DATABASE=PROD_ID_4VNKUnMlmlk' # имя БД Лореаль
user = 'UID=sll_mainuser'
pwd = 'PWD=TNzvgLAFprjerWKG'
conn_str = ';'.join([driver, server, port, db, user, pwd])

#соединяемся с БД
conn = pyodbc.connect(conn_str)

#сообщаем статус коннекта
print('Connect with ClientDB open')

#формируем курсор
c = conn.cursor()

# находим УВОЛЕННЫХ сотрудников в клиентской БД
#
query = """
SELECT
	profe.user_id,
	proff.name,
	proff.email
FROM profile_empl profe
INNER JOIN profile_fl proff ON profe.user_id = proff.user_id
WHERE profe.dismissal_date IS NOT NULL
"""

#выполняем запрос
c.execute(query)
rows = c.fetchall()

#сохраняем уволенных сотрудников в списке
#
dismiss_usr_list = []
for row in rows:
    dismiss_usr_list.append(row)

#вывод списка
#for usr in dismiss_usr_list:
#    print(usr)

#сохраняем список user_id для использования в фильтрации следующих запросов
#
user_id_list = []
for row in dismiss_usr_list:
	user_id_list.append(str(row[0]))

# debug
""" for id in user_id_list:
	print(id) """

conn.close() # закрываем соединение с клиентской БД
print('Connect with ClientDB close')


#***
# Соединяемся с системной DB
#
#соединяемся с БД
sys_conn = pymysql.connect(
		host="10.100.60.111",
		user="systemUser",
		passwd="S2j5~${P6ic",
		db="system",
		charset='utf8')

#информируем о соединение с системной БД
print('Connect with SystemDB open')

#формируем курсор
cur = sys_conn.cursor()

# получаем статусы сотрудников в системной БД
#
query = """
SELECT
	u.id,
	u.active,
	p_bx.id as bx_id
FROM user u INNER JOIN profile_bx p_bx
ON u.id = p_bx.user_id
WHERE u.id IN ({})""".format(','.join(['%s']*len(user_id_list)))

#выполняем запрос
cur.execute(query, tuple(user_id_list))
rows = cur.fetchall()

dismiss_usr_sys_active_list = [] # статусы активности сотрудников в системной БД
bitrix_id_list = [] # список хранящий битрикс id сотрудников для фильтрации в БД
for row in rows:
	dismiss_usr_sys_active_list.append(row)
	bitrix_id_list.append(row[2])

# тестовые операторы, удалить
#for id in bitrix_id_list:
#	print('bitrix_id: ' + str(id))


sys_conn.close()
print('Connect with SystemDB close')

#***
# Соединяемся с Битриксовой БД
#

#соединяемся с БД
bx_conn = pymysql.connect(
		host="10.100.60.111",
		user="bitrixUser",
		passwd="bziH#g9y5yw5J~",
		db="dev_bitrix_v3",
		charset='utf8')

#информируем о соединение с системной БД
print('Connect with BitrixDB open')

#формируем курсор
cur = bx_conn.cursor()

# получаем статусы сотрудников в системной БД
#
query = """
SELECT
	id,
	active
FROM b_user
WHERE id IN ({})""".format(','.join(['%s']*len(bitrix_id_list)))

#выполняем запрос
cur.execute(query, tuple(bitrix_id_list))
rows = cur.fetchall()

dismiss_usr_bx_active_list = [] # статусы активности сотрудников в битриксовой БД
for row in rows:
	dismiss_usr_bx_active_list.append(row)

#debug
""" for usr in dismiss_usr_bx_active_list:
	print(usr)
 """
bx_conn.close()

print('Connect with BitrixDB close')
select_time = datetime.today()


""" def list_with_find_value(lst, find_value, column_index):
	# отсекаем пустые списки и ошибочные номера столбцов
	if (len(lst) == 0) or (column_index > len(lst)-1):
		return -1

	for l in lst:
		if l[column_index] = find_value:
			return res = [x for x in l] """

"""
# ***
# Объединяем списки
#
common_dismiss_usr_list = []
for usr in dismiss_usr_list:
#	if usr[0] in dismiss_usr_sys_active_list:
#		common_dismiss_usr_list.append(usr + find_index_of_list())
	for sys_usr in dismiss_usr_sys_active_list:
		if usr[0] in sys_usr:
			common_dismiss_usr_list.append(usr + sys_usr[1:2])"""

temp_dismiss_usr_list = [] # временный список хранящий статусы сотрудников

# добавляем во временный список данные по статусам из клиентской и системной БД
#
for usr in dismiss_usr_list:
	for usr_sys in dismiss_usr_sys_active_list:
		if usr[0] in usr_sys: #ищем id уволенного сотрудника в выгрузке из системной БД
			temp_dismiss_usr_list.append(list(usr) + list(usr_sys[1:3])) # добавляем данные в результирующий список


common_dismiss_usr_list = [] # результирующий список хранящий статусы сотрудников

# заполняем результирующий список статусами из всех баз
#
for usr in temp_dismiss_usr_list:
	for usr_bx in dismiss_usr_bx_active_list:
		if usr[4] in usr_bx:
			common_dismiss_usr_list.append(list(usr) + list(usr_bx[1]))

#debug
for i in common_dismiss_usr_list:
	print(i)

finish_time = datetime.today()
diff_time = abs(finish_time - start_time).seconds
print('_______________________________________________________________________')
print('Start: ' + str(start_time) + ' Finish: ' + str(finish_time))
print('Select time: ' + str(diff_time) + ' second(s)')
print('All time work: ' + str(diff_time) + ' second(s)')
print('_______________________________________________________________________')
			


	


	