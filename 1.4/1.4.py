# -*- coding: utf-8 -*-
"""
Created on Sun Jul 21 17:46:35 2024

@author: dalyuki
"""

  
# Задача 1.4

# Создайте функцию (в Oracle или PostgreSQL), которая будет принимать дату, а 

# возвращать эту дату и информацию о максимальной и минимальной сумме проводки 

# по кредиту и по дебету за переданную дату. То есть эти значения надо вычислять 

# на основании данных в таблице «ds.ft_posting_f».

# Напишите скрипт на одном из 3-х языков (Python / Scala / Java), который будет

# вызывать разработанную ранее функцию и её ответ сохранять в csv-формате.
 
 

# Загрузим библиотеки
 
import pandas as pd
from sqlalchemy import create_engine
import numpy as np

 

# Создадим функцию, читающую данные из целевой таблицы и выполняющую вычисления

def ft_posting_func(date):
    
     
    # Установим подключение к базе данных 
     
    try:
        # Попытаемся подключиться к базе данных
        
        engine = create_engine("postgresql://postgres:1234@localhost:5432/postgres")
        
                 # В случае успеха будет выведено сообщение 
        print('Connected to database')
    
    except:
        # В случае сбоя подключения будет выведено сообщение в STDOUT
        print('Can`t establish connection to database')
    
    
    # Sql запрос для выгрузки таблицы «ds.ft_posting_f» в пандас   
    sql = "SELECT * FROM ds.ft_posting_f"
        
        
    # Создадим датафрейм
    
    # Типы данных в колонках
    cols_dtypes = {'id':int,'oper_date':str,
                   'credit_account_rk':int,
                   'debet_account_rk':int,
                   'credit_amount':float,
                   'debet_amount':float}
    
    df = pd.read_sql(sql,con=engine,dtype = cols_dtypes)
    
  
    # Считаем значения максимальной и минимальной сумме проводки 

    # по кредиту и по дебету за переданную дату

    # Сделаем выборку из полей "debet_amount" и "credit_amount" для нужной даты    
        
    credit_amount =  df.loc[df['oper_date'] == date, 'credit_amount']
     
    debet_amount = df.loc[df['oper_date'] == date, 'debet_amount'] 
      
    # Посчитаем максимальное и минимальное значения для каждой выборки
     
    max_credit_amount = np.max(credit_amount)
    
    min_credit_amount =  np.min(credit_amount)
    
    max_debet_amount = np.max(debet_amount)
    
    min_debet_amount = np.min(debet_amount)
    
    data_dict = {
        'oper_date':date,
        'max_credit_amount':[max_credit_amount],
        'min_credit_amount':[min_credit_amount],
        'max_debet_amount':[max_debet_amount],
        'min_debet_amount':[min_debet_amount]
        }
         
    df2 = pd.DataFrame(data_dict)
    
    
    # Сохраним результат в CSV
    
    save_dir = ("C:/Users/dalyuki.SAN-I-E14-253/Desktop/NeoFlex/"
                "Проектное задание/DE/1.4/calculations.csv")
     
    df2.to_csv(save_dir,index=False)
    
    
# Запустим функцию
    
ft_posting_func(date='2018-01-09')       
        