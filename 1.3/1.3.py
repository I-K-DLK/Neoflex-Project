  
# Напишите скрипт, который позволит выгрузить данные из витрины «dm.dm _f101_round_f» 

# в csv-файл, первой строкой должны быть наименования колонок таблицы.

# Убедитесь, что данные выгружаются в корректном формате и напишите скрипт, 

# который позволит импортировать их обратно в БД. Поменяйте пару значений 

# в выгруженном csv-файле и загрузите его в копию таблицы 101-формы «dm.dm _f101_round_f_v2».

# Постарайтесь покрыть данные процессы простым логированием. Скрипты можно 

# написать на Python / Scala / Java.


# Загрузим библиотеки
 
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, select, Table, Column
from sqlalchemy import Integer, String, MetaData, text 
from sqlalchemy.sql.expression import literal
import sqlalchemy as sa
from sqlalchemy import insert  

# Укажем рабочую директорию для выгрузки csv из БД

path = """C:/Users/dalyuki.SAN-I-E14-253/Desktop/NeoFlex/Проектное задание/DE/1.3/table.csv"""

# Cоздаем таблицу для хранения логов вставки данных  
   
metadata = MetaData()
  
log_table = Table(
"logs",
metadata,
Column("record_id", Integer, primary_key=True),
Column("start_time", String),
Column("end_time", String),
Column("operation", String),
Column("pid", String),
Column("usename", String),
Column("client_addr", String),
Column("state", String),
Column("query", String),
Column("backend_type", String),
schema="dm"
)
      

# Установим подключение к базе данных 
 
try:
    # Попытаемся подключиться к базе данных
    
    
    engine = create_engine("postgresql://postgres:1234@localhost:5432/postgres")
            
    # В случае успеха будет выведено сообщение 
    print('Connected to database')
    
   
    # Создадим таблицы
    
    metadata.create_all(engine)
     
except:
    # В случае сбоя подключения будет выведено сообщение 
    print('Can`t establish connection to database')
      


# Чтение данных из БД таблицы "dm.dm_f101_round_f"


# Установим подключение к базе данных 
 
try:
    # Попытаемся подключиться к базе данных
        
    engine = create_engine("postgresql://postgres:1234@localhost:5432/postgres")
        
    # В случае успеха будет выведено сообщение 
    
    print('Connected to database')
    
    
    # Sql запрос для выгрузки таблицы в пандас
           
    query = "select * from dm.dm_f101_round_f"
     
    # Время начала чтения

    start_time = str(datetime.now())
     
    # Создадим датафрейм
      
    df = pd.read_sql(query,con=engine)
    
    # Прочитаем типы данных из таблицы
    columns = df.columns
    
    dtypes = dict()
    
    for c in columns:
        if  c == "from_date" or c =="to_date":
            dtypes[c] =  sa.types.DATE()
        elif c == "chapter" or c =="characteristic":
            dtypes[c] =  sa.types.CHAR(length=1)
        elif c == "ledger_account":
            dtypes[c] =  sa.types.CHAR(length=5) 
        else: dtypes[c] = sa.types.Numeric(precision = 23,scale=8)  
        
   
    
    # Время конца чтения
        
    end_time = str(datetime.now())
     
    # Тип операции 
    
    operation_type = "Reading"
    
   # Запрос получения данных для записи в логи из таблицы  'pg_stat_activity'
   
    write_log_stmt = text(
        """
        SELECT pid, usename, client_addr, state,query,backend_type 
        FROM pg_stat_activity 
        WHERE pid = pg_backend_pid()
        """)
     
        
    # Колонки в запросе вставки из таблицы  'pg_stat_activity'
    
    write_log_stmt = write_log_stmt.columns(Column('pid'), 
                                            Column('usename'), 
                                            Column('client_addr'), 
                                            Column('state'), 
                                            Column('query'), 
                                            Column('backend_type')
                                            ).subquery('pg_stat_activity')
    
    # Select запрос который будет использован в подзапросе вставки данных
    
    select_to_insert = select([literal(start_time).label('start_time'),
                                literal(end_time).label('end_time'),
                                literal(operation_type).label('operation')] 
                                + list(write_log_stmt.columns))
    
    # Сам подзапрос
    
    subquery = select_to_insert.subquery()
     
    
    # Запрос вставки в логи
    
    write_log_query = insert(log_table).from_select(select_to_insert.selected_columns,
                                                      select_to_insert)
     

    
    
    with engine.connect() as connection:
        with connection.begin():     
            
            connection.execute(write_log_query) 
    
    
     
except:
    # В случае сбоя подключения будет выведено сообщение 
    
    print('Can`t establish connection to database')
       
 
# Запишем датафрейм в csv   
 
df.to_csv(path,encoding = 'utf-8')


# Читаем csv и заменим пару значений для теста в 1 строке поменяем значение
# balance_in_val,а во второй balance_in_total на 100000 и 250000 соответственно


df_2 = pd.read_csv(path,index_col=0)

df_2.iloc[0, df_2.columns.get_loc('balance_in_val')] = 100000 

df_2.iloc[1, df_2.columns.get_loc('balance_in_total')] = 250000 


# Загрузим csv в новую таблицу в БД

# Cоздаем копию исходной таблицы

create_table_query = text(
"""create table if not exists dm.dm_f101_round_f_v2
(like dm.dm_f101_round_f including all);""")

# Установим подключение к базе данных 
 
try:
    # Попытаемся подключиться к базе данных
    
    engine = create_engine("postgresql://postgres:1234@localhost:5432/postgres")    
 
    # В случае успеха будет выведено сообщение 
    
    print('Connected to database')
    
    with engine.connect() as connection:
        with connection.begin():     
     
            connection.execute(create_table_query)

except:
    # В случае сбоя подключения будет выведено сообщение 
    
    print('Can`t establish connection to database')
     

# Запишем данные в новую таблицу из файла  CSV
    
      
# Установим подключение к базе данных 
 
try:
    # Попытаемся подключиться к базе данных
    
    engine = create_engine("postgresql://postgres:1234@localhost:5432/postgres")
    
     
    # В случае успеха будет выведено сообщение 
    
    print('Connected to database')
    
    
    # Время начала чтения
    
    start_time = str(datetime.now())
    
    # Перенесем данные из датафрейма в таблицу "dm_f101_round_f_v2"
    
     
    with engine.connect() as connection:
        with connection.begin():     
             
            df_2.to_sql(name='dm_f101_round_f_v2',schema='dm',con=engine,
                        if_exists='replace', index=0, method='multi',dtype=dtypes)
         
    # Время конца чтения
        
    end_time = str(datetime.now())
    
    # Тип операции 
    
    operation_type = "Writing"
    
    # Внесем информацию о чтении данных в логовую таблицу
      
     
    # Запрос получения данных для записи в логи из таблицы  'pg_stat_activity' 
    
    write_log_stmt = text(
        """
        SELECT pid, usename, client_addr, state,query,backend_type 
        FROM pg_stat_activity 
        WHERE pid = pg_backend_pid()
        """)
    
    # Далее аналогично предыдущему коду..
     
    write_log_stmt = write_log_stmt.columns(Column('pid'), 
                                            Column('usename'), 
                                            Column('client_addr'), 
                                            Column('state'), 
                                            Column('query'), 
                                            Column('backend_type')
                                            ).subquery('pg_stat_activity')
      
    select_to_insert = select([literal(start_time).label('start_time'),
                                literal(end_time).label('end_time'),
                                literal(operation_type).label('operation')] 
                                + list(write_log_stmt.columns))
     
    subquery = select_to_insert.subquery()
     
    write_log_query = insert(log_table).from_select(select_to_insert.selected_columns,
                                                      select_to_insert)
   
     
     
    with engine.connect() as connection:
        with connection.begin():     
            
            connection.execute(write_log_query) #worksssss

except:
    # В случае сбоя подключения будет выведено сообщение 
    
    print('Can`t establish connection to database')     