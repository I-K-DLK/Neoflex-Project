-- Подготовка
-- Создадим  новую схему DS -- Слой детальных данных
 
create schema ds;
 
-- Создадим  новую схему LOGS -- Логи

create schema logs; 

-- добавим пустые таблицы для заполнения данными
-- Тип данных number заменим на numeric, varchar2 на varchar (https://habr.com/ru/articles/335716/)

-- 1)

drop table if exists ds.md_ledger_account_s;

create table ds.md_ledger_account_s(
chapter char(1),
chapter_name varchar(16),
section_number integer,
section_name varchar(22),
subsection_name varchar(21),
ledger1_account integer,
ledger1_account_name varchar(47),
ledger_account integer not null,
ledger_account_name varchar(153),
characteristic char(1),
is_resident integer,
is_reserve integer,
is_reserved integer,
is_loan integer,
is_reserved_assets integer,
is_overdue integer,
is_interest integer,
pair_account varchar(5),
start_date date not null,
end_date date,
is_rub_only integer,
min_term varchar(1),
min_term_measure varchar(1),
max_term varchar(1),
max_term_measure varchar(1),
ledger_acc_full_name_translit varchar(1),
is_revaluation varchar(1),
is_correct varchar(1),
primary key(ledger_account, start_date)
);

-- 2)

drop table if exists ds.md_exchange_rate_d;

create table ds.md_exchange_rate_d(
id int not null,
data_actual_date date not null,
data_actual_end_date date,
currency_rk numeric not null,
reduced_cource float,
code_iso_num varchar(3),
primary key(id,data_actual_date, currency_rk)
);

-- 3)

drop table if exists ds.md_currency_d;

create table ds.md_currency_d(
currency_rk numeric not null,
data_actual_date date not null,
data_actual_end_date date,
currency_code varchar(3),
code_iso_char varchar(3),
primary key(currency_rk, data_actual_date)
);

-- 4)

drop table if exists ds.md_account_d;

create table ds.md_account_d(
data_actual_date date not null,
data_actual_end_date date not null,
account_rk numeric not null,
account_number varchar(20) not null,
char_type varchar(1) not null,
currency_rk numeric not null,
currency_code varchar(3) not null,
primary key(data_actual_date, account_rk)
);

-- 5)

/*Поскольку исходные данные не позволяют идентирфицировать каждую запись  таблице из-за дубликатов первичного ключа
 мы добавляем дополнительную колонку id для каждой записи со знаяениями, равными номеру строки при загрузке*/ 
drop table if exists ds.ft_posting_f;

create table ds.ft_posting_f(
id int not null,
oper_date date not null,
credit_account_rk numeric not null,
debet_account_rk numeric not null,
credit_amount float,
debet_amount float,
primary key(id, oper_date, credit_account_rk, debet_account_rk)
);

-- 6)
 
drop table if exists ds.ft_balance_f;

create table ds.ft_balance_f(
on_date date not null,
account_rk numeric not null,
currency_rk numeric,
balance_out float,
primary key(on_date, account_rk)
);

-- 7) 
 
-- Добавим таблицу, куда будут записываться доги

drop table if exists logs.insert_log; 

create table logs.insert_log(
id serial not null,
insert_start_time timestamp not null,
insert_end_time timestamp not null,
table_of_insert text not null,
insert_size numeric not null,
time_cost text not null,
primary key(id)
); 

select * from ds.ft_balance_f;

select * from ds.ft_posting_f;

select * from ds.md_account_d;

select * from ds.md_currency_d mcd ;

select * from ds.md_exchange_rate_d merd ;

select * from ds.md_ledger_account_s mlas ;

-- Проверка вставки в логах

select * from logs.insert_log
