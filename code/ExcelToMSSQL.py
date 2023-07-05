import pyodbc
import sqlalchemy
import pandas as pd
import urllib
from datetime import datetime as dt

# импорт библиотек

class MissingValue(Exception):
    pass

# класс для создания ошибки пропущенных значений в датафрейме


class Sql:
    def __init__(self, database, server="DESKTOP-ES2QHD6"):
        self.cnxn = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                                   "Server="+server+";"
                                   "Database="+database+";"
                                   "Trusted_Connection=yes;")
        self.conn = "-- {} -- successfully".format(datetime.now()
                                                         .strftime("%d/%m/%Y"))
    def query(self, req):
        cursor = sql.cnxn.execute(req)
        row = cursor.fetchone()
        while row:
            print(row[0])
            row = cursor.fetchone()

# создание класса для подключения к бд
# и совершения запросов к ней


def extract_data(file_path="data.xlsx"):
    print("---Начало процесса extract---")
    try:
        df_employee = pd.read_excel(file_path, sheet_name='employee')
        print("Успешно загружен лист - employee")

    except ValueError:
        print("Лист employee не найден")
        raise

    except:
        print("Что-то пошло не так с листом employee")
        raise

    try:
        df_customer = pd.read_excel(file_path, sheet_name='customer')
        print("Успешно загружен лист - customer")

    except ValueError:
        print("Лист customer не найден")
        raise

    except:
        print("Что-то пошло не так с листом customer")
        raise

    try:
        df_product = pd.read_excel(file_path, sheet_name='product')
        print("Успешно загружен лист - product")

    except ValueError:
        print("Лист product не найден")
        raise

    except:
        print("Что-то пошло не так с листом product")
        raise

    try:
        df_order = pd.read_excel(file_path, sheet_name='order')
        print("Успешно загружен лист - order")

    except ValueError:
        print("Лист order не найден")
        raise

    except:
        print("Что-то пошло не так с листом order")
        raise

    print('---Конец процесса extract---')


    return df_employee, df_customer, df_order, df_product

# загрузка данных из excel файла


def transform_data(df_employee, df_customer, df_product, df_order):
    print('---Начало процесса transform---')

    if sum(list(df_employee.isnull().sum())) == 0:
        print('Отсутствуют пустые значения - df_employee')
    else:
        raise MissingValue('Пустые значения в df_employee')
    if sum(list(df_customer.isnull().sum())) == 0:
        print('Отсутствуют пустые значения - df_customer')
    else:
        raise MissingValue('Пустые значения в df_customer')
    if sum(list(df_product.isnull().sum())) == 0:
        print('Отсутствуют пустые значения - df_product')
    else:
        raise MissingValue('Пустые значения в df_product')
    if sum(list(df_order.isnull().sum())) == 0:
        print('Отсутствуют пустые значения - df_order')
    else:
        raise MissingValue('Пустые значения в df_order')
    # проверка столбцов на пустые значения


    df_order.order_date = df_order.order_date.apply(lambda x: dt.date(x))
    df_employee.hire_date = df_employee.hire_date.apply(lambda x: dt.date(x))

    # перевод типов столбцов из timestamp в datetime


    df_date = pd.DataFrame(columns=['id', 'the_date', 'day_of_week', 'day_of_month', 'day_of_year', 'day_name',
                                    'month_of_year', 'month_name', 'weekend', 'quarter', 'year'])


    # создание таблицы для дат заказов


    def is_weekend(n):
        if (int(n) == 5) or (int(n) == 6):
            return 1
        return 0


    # функция, которая проверяет является ли день субботой или воскресеньем


    def cal_quarter(n):
        if int(n) in [1, 2, 3]:
            return 1
        elif int(n) in [4, 5, 6]:
            return 2
        elif int(n) in [7, 8, 9]:
            return 3
        elif int(n) in [10, 11, 12]:
            return 4


    # функция, которая определяет квартал которому принадлежит дата
    try:
        cursor = sql.cnxn.execute("SELECT id FROM dim_date ORDER BY id DESC")
        row = cursor.fetchone()
        num_id = row[0]
    except:
        num_id = 0

    for n in range(len(df_order)):
        s = df_order.order_date[n].strftime('%w, %d, %j, %A, %m, %B, %Y')
        s = s.split(',')
        weekend = is_weekend(s[0])
        quarter = cal_quarter(s[4])
        df_date = df_date.append(
            {'id': num_id + 1, 'the_date': df_order.order_date[n], 'day_of_week': int(s[0]) + 1, 'day_of_month': int(s[1]),
             'day_of_year': int(s[2]), 'day_name': s[3], 'month_of_year': int(s[4]),
             'month_name': s[5], 'weekend': weekend, 'quarter': quarter, 'year': int(s[6])}, ignore_index=True)
        num_id += 1
        df_order.order_date[n] = n + 1
    df_order = df_order.rename(columns={'order_date': 'date_id'})

    # запись данных в новый датафрейм df_date

    print('---Конец процесса transform---')
    return df_employee, df_customer, df_order, df_product, df_date

# обработка данных


def load_data(df_employee, df_customer, df_order, df_product, df_date):
    print('---Начало процесса load---')
    quoted = urllib.parse.quote_plus("DRIVER={SQL Server};SERVER=DESKTOP-ES2QHD6;DATABASE=BasisProDW")
    engine = sqlalchemy.create_engine('mssql+pyodbc:///?odbc_connect={}'.format(quoted))
    # начало загрузки датафрейма
    with engine.connect() as conn:
        transaction = conn.begin()
        try:
            df_employee.to_sql("dim_employee", con=engine, if_exists='append', index=False)
            transaction.commit()
            print('Успешная загрузка датафрейма - df_employee')

        except Exception as E:
            print(E)
            transaction.rollback()
            raise
    # конец загрузки датафрейма
    # данная конструкция создана для добавления данных транкзакциями
    # для обеспечения целостности данных

    with engine.connect() as conn:
        transaction = conn.begin()
        try:
            df_product.to_sql("dim_product", con=engine, if_exists='append', index=False)
            transaction.commit()
            print('Успешная загрузка датафрейма - df_product')

        except Exception as E:
            print(E)
            transaction.rollback()
            raise

    with engine.connect() as conn:
        transaction = conn.begin()
        try:
            df_customer.to_sql("dim_customer", con=engine, if_exists='append', index=False)
            transaction.commit()
            print('Успешная загрузка датафрейма - df_customer')

        except Exception as E:
            print(E)
            transaction.rollback()
            raise

    with engine.connect() as conn:
        transaction = conn.begin()
        try:
            df_order.to_sql("fact_sales", con=engine, if_exists='append', index=False)
            transaction.commit()
            print('Успешная загрузка датафрейма - df_order')

        except Exception as E:
            print(E)
            transaction.rollback()
            raise


    with engine.connect() as conn:
        transaction = conn.begin()
        try:
            df_date.to_sql("dim_date", con=engine, if_exists='append', index=False)
            transaction.commit()
            print('Успешная загрузка датафрейма - df_date')

        except Exception as E:
            print(E)
            transaction.rollback()
            raise
    print('---Конец процесса load---')
# загрузка данных в хранилище


if __name__ == "__main__":
    sql = Sql("BasisProDW")
    sql.conn
    df_employee, df_customer, df_order, df_product = extract_data()
    df_employee, df_customer, df_order, df_product, df_date = \
        transform_data(df_employee, df_customer, df_product, df_order)
    load_data(df_employee, df_customer, df_order, df_product, df_date)
