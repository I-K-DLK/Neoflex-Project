
# 1. Покажите как вы в приложении указываете путь до директрии с дельтами, 
# наименование таблицы и список полей являющихся ключами (для таблицы счетов, 
# оно одно - id счëта) ;
# Запустите приложение и покажите как выводится (через df.show()) промежуточное 
# состояние зеркала и конечное. Откройте финальный csv-файл, который сохранился 
# в директорию "md_account";
# Покажите какая информация записалась в логи;
# Создайте сами ещё одну дельту (то есть ещё одну директорию с ещё одним 
# csv-файликом, где вы укажите существующий счёт, но измените его вторичный 
# параметр – client_id / branch_id / open_in_internet);
# Запустите ваше Spark-приложение повторно и покажите, что, опираясь на логи оно 
# не будет повторно грузить все дельты и применит только вашу новую дельту;
# Покажите финальный csv-файл с обновлённым значением у счёта;

# Import the necessary modules

import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as f 

# Create a SparkSession
spark = SparkSession.builder.appName("Task 2.2").getOrCreate()

spark = (SparkSession
 .builder
 .appName('Task 2.2')
 .enableHiveSupport()
 .getOrCreate())
 


# Choose directories for logs, mirror table and deltas
 
log_path = "/home/ubuntu/Desktop/shared_2.2/logs/logs.csv"

mirror_path = "/home/ubuntu/Desktop/shared_2.2/mirr_md_account_d/"

deltas_dir = "/home/ubuntu/Desktop/shared_2.2/data_deltas/"

# Define the name of mirror csv

mirror_file_name = "md_account_d"

extension = ".csv"

# Choose the column that has to be unique

unique_column = 'ACCOUNT_NUMBER'

# Create dict with delta names directories

delta_dict = {}
file_list = os.listdir(deltas_dir)
with os.scandir(deltas_dir) as subdirs:
    delta_subdirs = sorted([entry.name for entry in subdirs])
    
nums = list(enumerate(delta_subdirs,1))  


for ds in delta_subdirs:
        
    delta_dict[ds] = deltas_dir+ds
            

# Check if mirror table exists. If not - read the first delta 
# and save as a mirror 
 
isExisting = os.path.exists(mirror_path+mirror_file_name+extension)

if not isExisting:
    
    
    # Find the first Delta directory and load it
    
    df = spark.read.option("delimiter", ";").option("header", "true").csv(deltas_dir+delta_subdirs[0])
    
    df.show()
    
    # Write the first delta as a mirror
    
    # Time of beginning
    
    time_start = datetime.now()
      
    
    df.toPandas().to_csv(mirror_path+mirror_file_name+extension, sep=";" , header=True, index=False)

    
    # Time of ending
    
    time_end = datetime.now()
    
    # Write information about 1st delta in a log table
    
    log_insert = [time_start,time_end, delta_subdirs[0],mirror_file_name]
    
    # Create  log dataframe
    
    cols = ['time_start','time_end','delta_directory','table_name']
 
    df_log = spark.createDataFrame([log_insert],cols)
     
    df_log.show()
    
    # Save it
    
    df_log.toPandas().to_csv(log_path, sep=";" , header=True, index=False)
 
else:
     
      # Extract the column with delta numbers
     
      delta_num = spark.read.option("delimiter", ";").option("header", "true").csv(log_path)
      
      # Read the last Delta file loaded
      
      last_delta_num = delta_num.select('delta_directory').orderBy('delta_directory',ascending = False).limit(1)
        
      # Find the last loaded Delta directory and load the next one
       
      df_delta =  last_delta_num.head()[0]
    
      next_delta = str(int(df_delta)+1)
      
      df_next_delta = spark.read.option("delimiter", ";").option("header", "true").csv(deltas_dir+next_delta)

      # Insert data from new Delta to the mirror file
      
      # First read the mirror table
      
      df_mirror = spark.read.option("delimiter", ";").option("header", "true").csv(mirror_path+mirror_file_name+extension)
      
      # Show it
      
      df_mirror.show()
       
      df_mirror_new = df_mirror.alias('mirror').join(
      df_next_delta.alias('delta'), ['ACCOUNT_NUMBER'], how='outer'
      ).select( 
        f.coalesce('delta.DATA_ACTUAL_DATE', 'mirror.DATA_ACTUAL_DATE').alias('DATA_ACTUAL_DATE'), 
        f.coalesce('delta.DATA_ACTUAL_END_DATE', 'mirror.DATA_ACTUAL_END_DATE').alias('DATA_ACTUAL_END_DATE'),
        f.coalesce('delta.ACCOUNT_RK', 'mirror.ACCOUNT_RK').alias('ACCOUNT_RK'), 
        'ACCOUNT_NUMBER', 
        f.coalesce('delta.CHAR_TYPE', 'mirror.CHAR_TYPE').alias('CHAR_TYPE'), 
        f.coalesce('delta.CURRENCY_RK', 'mirror.CURRENCY_RK').alias('CURRENCY_RK'), 
        f.coalesce('delta.CURRENCY_CODE', 'mirror.CURRENCY_CODE').alias('CURRENCY_CODE'), 
        f.coalesce('delta.CLIENT_ID', 'mirror.CLIENT_ID').alias('CLIENT_ID'),
        f.coalesce('delta.BRANCH_ID', 'mirror.BRANCH_ID').alias('BRANCH_ID'),
        f.coalesce('delta.OPEN_IN_INTERNET', 'mirror.OPEN_IN_INTERNET').alias('OPEN_IN_INTERNET')
      )
    
      df_mirror_new.show()
       
      print('new mirror')   
    # Write current delta as a mirror
    
    # Time of writing start
    
      time_start = datetime.now()
      
      df_mirror_new.toPandas().to_csv(mirror_path+mirror_file_name+extension, sep=";" , header=True, index=False)

      # Time of  writing end
    
      time_end = datetime.now()
    
    # Write information about new delta in the log table
    
      log_insert = [time_start,time_end, next_delta,mirror_file_name]
    
    # Create  log dataframe
    
    
      cols = ['time_start','time_end','delta_directory','table_name']
 
    
      df_log = spark.createDataFrame([log_insert],cols)
     
      df_log.show()
    
    # Save it
    
      df_log.toPandas().to_csv(log_path, sep=";" , header=False, index=False, mode="a" )
      
       