# Скрипт выполняет чтение данных из заданных файлов  CSV
# и производит загрузку и логирование зпгрузки данных в БД Postgres

  
# Загрузим библиотеки
 
import pandas as pd
from datetime import datetime
import time
from sqlalchemy import create_engine
from sqlalchemy import text


# Укажем рабочую директорию с файлами csv для загрузки в БД

workingdir = "C:/Users/dalyuki.SAN-I-E14-253/Downloads/задача_1.1/files_to_load/"

# Получим список файлов csv

from os import listdir

from os.path import isfile, join



file_list = [f for f in listdir(workingdir) if isfile(join(workingdir, f))]

# Установим подключение к базе данных 
 
try:
    # Попытаемся подключиться к базе данных
    
    engine = create_engine("postgresql://postgres:1234@localhost:5432/postgres")
    
     
    # В случае успеха будет выведено сообщение 
    print('Connected to database')

except:
    # В случае сбоя подключения будет выведено сообщение
    print('Can`t establish connection to database')
 
    
    
# Создадим словарь, элементами которого будут название таблицы в виде ключа 
# и колонки, не являющиеся первичным ключом, в качестве значений


table_excluded_cols_dict = {\
"ft_balance_f":"currency_rk,balance_out", "ft_posting_f":"credit_amount,debet_amount",\
"md_account_d":"data_actual_end_date, account_number,char_type,currency_rk,currency_code",\
"md_currency_d":"data_actual_end_date, currency_code,code_iso_char",\
"md_exchange_rate_d":"data_actual_end_date,reduced_cource,code_iso_num",\
"md_ledger_account_s":"chapter,chapter_name,section_number,section_name,subsection_name,\
ledger1_account,ledger1_account_name,ledger_account_name,characteristic,is_resident,is_reserve,\
is_reserved,is_loan,is_reserved_assets,is_overdue,is_interest,pair_account,end_date,is_rub_only,\
min_term,min_term_measure,max_term,max_term_measure,ledger_acc_full_name_translit,is_revaluation,is_correct"\
}

# Создадим словарь, элементами которого будут названия таблиц в качестве ключей
# и соответствующие названия читаемых колонок из файла CSV в качестве значений
   
table_cols_to_use = {\
"ft_balance_f":"on_date,account_rk,currency_rk,balance_out",\
"ft_posting_f":"id,oper_date,credit_account_rk,debet_account_rk,credit_amount,debet_amount",
"md_account_d":"data_actual_date,data_actual_end_date,account_rk,account_number,char_type,currency_rk,currency_code",\
"md_currency_d":"currency_rk,data_actual_date,data_actual_end_date,currency_code,code_iso_char",\
"md_exchange_rate_d":"id,data_actual_date,data_actual_end_date,currency_rk,reduced_cource,code_iso_num",\
"md_ledger_account_s":"chapter,chapter_name,section_number,section_name,subsection_name,ledger1_account,\
ledger1_account_name,ledger_account,ledger_account_name,characteristic,is_resident,is_reserve,is_reserved,\
is_loan,is_reserved_assets,is_overdue,is_interest,pair_account,start_date,end_date,is_rub_only,min_term,\
min_term_measure,max_term,max_term_measure,ledger_acc_full_name_translit,is_revaluation,is_correct"\
}

    
# Создадим словарь, элементами которого будут названия таблиц в качестве ключей
# и соответствующие названия колонок которые не могут содержать Null   

not_null_cols = {\
"ft_balance_f":"on_date,account_rk","ft_posting_f":"oper_date,credit_account_rk,debet_account_rk",\
"md_account_d":"data_actual_date,data_actual_end_date,account_rk,account_number,char_type,currency_rk,\
currency_code","md_currency_d":"currency_rk,data_actual_date","md_exchange_rate_d":"id,data_actual_date,\
currency_rk","md_ledger_account_s":"ledger_account,start_date"\
}

# Разобьем строки с именами колонок в виде листа

keys = not_null_cols.keys()


for key in not_null_cols.keys():
    
    not_null_cols[key] = not_null_cols[key].split(",")  

# Создадим функцию для вставки данных  во временную и целевую таблицы
 
def temp_table_create(table_name,cols_excluded,schema,conn,count):
    
     
    create_query =  "create temp table temporary_table (like " +\
        schema + table_name + " including all);"
    
        
    insert_query = "insert into " + schema+table_name +\
        " select * from temporary_table on conflict on constraint "+table_name+"_pkey\
            do update set " + cols_excluded +";"    
       
    # Получим текущую дату и время до загрузки
    
    time_before_insert = datetime.now()
      
    # Создадим временную таблицу
     
    with conn.connect() as connection:
        with connection.begin():   
            connection.execute(text(create_query))
                
           
      
        # Заполним временную таблицу данными
              
    df.to_sql(name = "temporary_table",con=engine,\
          if_exists='append', index=False, method='multi')    
    
            
    # Выполним вставку данных в целевую таблицу из временной         
    with engine.connect() as connection:
        with connection.begin():
                  
            connection.execute(text(insert_query))
          
         # Ждем 5 секунд
    
    time.sleep(5)
    
    # Получим текущую дату и время после загрузки
    
    time_after_insert = datetime.now()
    
    # Получим текущую дату и время после загрузки,  время загрузки, 
    # текст запроса и размер вставляемой таблицы
     
    
    insert_start_time = "TIMESTAMP" + " " + "'" + str(time_before_insert)+ "'"
    insert_end_time = "TIMESTAMP" + " " + "'" + str(time_after_insert)+ "'"
    table_of_insert = "'" + table_name + "'"
    insert_size = str(len(df)-1)
    time_cost_sec = "'"+ str(time_after_insert - time_before_insert) + "'"
        
    # Запишем в лог результаты
    
    separator = ', ' 
 
    col_list = [insert_start_time,insert_end_time,table_of_insert,insert_size,\
            time_cost_sec]

    log_insert_query = "insert into logs.insert_log(\
    insert_start_time,insert_end_time,table_of_insert,insert_size,time_cost)\
    values(" + separator.join(col_list) + ");"
    print(log_insert_query)
    
            
    # Выполним вставку данных в логовую таблицу 
    
    with engine.connect() as connection:
        
        with connection.begin():
            
            connection.execute(text(log_insert_query))
            
    # Выполним удаление временной таблицы
      
    drop_query = "drop table if exists temporary_table;"
       
    with engine.connect() as connection:
        
        with connection.begin():    
            
            connection.execute(text(drop_query))
          
            print("Table dropped!") 
       
        
for key in table_excluded_cols_dict.keys():
    
     
     
     value_array = table_excluded_cols_dict[key].split(",")
              
     value_array = [v.replace(v, v + " = excluded." + v) for v in value_array]
     
     value_string = ','.join(value_array)
        
     table_excluded_cols_dict[key] = value_string  
       

# Создадим словарь, где ключи будут именами файлов для выгрузки данных,
# а значения - тип кодировки файла

keys = file_list
vals = ["utf-8","utf-8","utf-8","utf-8","utf-8","cp866"]
encoding_dict = dict(zip(keys, vals))
  
 

    
count = 1
 
# Откроем csv файл 

for  f in file_list:
     
   
        
        
        index = f.rfind(".")
        
        schema = "ds."
        table_name = f[:index]
         
        
        
        cols_to_use =  table_cols_to_use[table_name].split(",")
        print(workingdir+f)
       
        
        # Прочитаем CSV файл в датафрейм
        
        df = pd.read_csv(workingdir+f,names = cols_to_use, delimiter=";",\
                         header = 0,encoding = encoding_dict[f])
            
        # Переведем названия полей в нижний регистр
        
        df.columns = df.columns.str.lower()
        
        # Если наш файл "md_ledger_account_s.csv" то
        # уберем дробную часть значений для поля "pair_account", 
        # чтобы в нашей БД оно умещалось в 5 символов
             
        if f == "md_ledger_account_s.csv":
        
            df['pair_account'] = df['pair_account'].astype(str).replace('\.0', '',\
                                                                        regex=True)
  
        # Удалим строки с Null значениями
        
        df.dropna(subset = not_null_cols[table_name],axis=0,how='any')

        # Время начала вставки данных
        
        start_time = time.time() # get start time before insert
                
        # Вставка данных
        
        temp_table_create(table_name, table_excluded_cols_dict[table_name],\
                          schema,engine, count)
       
        count = count + 1
        
        # Время конца и общее время вставки данных
        
        end_time = time.time()  
        total_time = end_time - start_time 
        print(f"Insert time: {total_time} seconds")  
        
    