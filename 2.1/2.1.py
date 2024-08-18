
# 1. Сгенерировать DataFrame из трёх колонок (row_id, discipline, season)  
# - олимпийские дисциплины по сезонам.
# •	row_id - число порядкового номера строки;
# •	discipline - наименование олимпиский дисциплины на английском 
# (полностью маленькими буквами);
# •	season - сезон дисциплины (summer / winter);
# *Укажите не мнее чем по 5 дисциплин для каждого сезона.
# Сохраните DataFrame в csv-файл, разделитель колонок табуляция, первая строка 
# должна содержать название колонок.
# Данные должны быть сохранены в виде 1 csv-файла а не множества маленьких.

# Import the mecessary modules

from pyspark.sql import SparkSession
 
# Create a SparkSession
spark = SparkSession.builder.appName("Task 2.1").getOrCreate()

spark = (SparkSession
 .builder
 .appName('Task 2.1')
 .enableHiveSupport()
 .getOrCreate())


# type some data 
data = [
(1,"badminton", "summer"),(2,"swimming", "summer"),
(3,"boxing", "summer"), (4,"rowing", "summer"),
(5,"kenny", "summer"), (6,"sailing", "summer"),
(7,"hockey", "winter"),(8,"curling", "winter"),
(9,"biathlon", "winter"), (10,"ski jumping", "winter"),
(11,"freestyle", "winter"), (12,"luge", "winter")
]


# Create a dataframe

schema = "row_id BIGINT, discipline STRING, season STRING"

sport_df = spark.createDataFrame(data, schema)

# Show it

sport_df.show() 

# Choose directories to save data

path = "/home/ubuntu/Desktop/shared/sport_df"

pandas_path = "/home/ubuntu/Desktop/shared/sport_df.csv"


# Save data to csv files

sport_df.repartition(1).write.option("header", "true").option("sep", "\t").csv(path,mode="overwrite")

sport_df.toPandas().to_csv(pandas_path, sep="\t" , header=True, index=False)



# 2. Прочитайте исходный файл "Athletes.csv".
# Посчитайте в разрезе дисциплин сколько всего спортсменов в каждой из дисциплин принимало участие.
# Результат сохраните в формате parquet. 

# Choose a directory to load data

path_to_load = "/home/ubuntu/Desktop/shared/Athletes.csv"

# Read it

athletes_df = spark.read.option("delimiter", ";").option("header", "true").csv(path_to_load)

# Show it

athletes_df.show() 


# DataFrames can be saved as Parquet files, maintaining the schema information.
athletes_df.write.parquet("athletes_df.parquet",mode="overwrite")

# Read in the Parquet file created above.
 
parquet_athletes = spark.read.parquet("athletes_df.parquet")


# Create a temporary view and then used in SQL statement

parquet_athletes.createOrReplaceTempView("parquet_athletes")


discipline_count = spark.sql("SELECT lower(discipline) as discipline, "
                              "COUNT(discipline) as count "
                              "FROM parquet_athletes GROUP BY discipline "
                              "ORDER BY discipline")

# Show it 

discipline_count.show()


# # 3. Прочитайте исходный файл "Athletes.csv".
# Посчитайте в разрезе дисциплин сколько всего спортсменов в каждой из дисциплин принимало участие.
# Получившийся результат нужно объединить с сгенерированным вами DataFrame из 1-го задания и в итоге
# вывести количество участников, только по тем дисциплинам, что есть в вашем сгенерированном DataFrame.
# Результат сохраните в формате parquet.

# Write the parquet


path_to_load = "/home/ubuntu/Desktop/shared/sport_df.csv"

sport_df = spark.read.option("delimiter","\t").option("header","true").csv(path_to_load)

 
sport_df.write.parquet("sport_df.parquet",mode="overwrite")


# Read in the Parquet our previously created dataframe
 
parquet_sport = spark.read.parquet("sport_df.parquet")


# Combine results of both dataframe and count the number of sportsmen in each
# discipline presenting in "sport_df"

# Create a temporary view  of "sport_df" and then use it in SQL statement

parquet_sport.createOrReplaceTempView("parquet_sport")

parquet_sport.show()

discipline_summary = spark.sql("with cte as (SELECT lower(discipline) "
                                             "as discipline "
                                             "FROM parquet_athletes "
                                             "UNION ALL "
                                             "SELECT discipline "
                                             "FROM parquet_sport) "
                                             "SELECT discipline, "
                                             "COUNT(discipline) as count "
                                             "FROM cte "
                                             "GROUP BY discipline "
                                             "HAVING discipline IN "
                                             "(SELECT discipline "
                                              "FROM parquet_sport)"
                                 )
    
discipline_summary.show()