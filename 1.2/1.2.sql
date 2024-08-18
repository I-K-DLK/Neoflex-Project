-- Подготовка

-- Создадим  новую схему DM -- Слой витрин 
create schema dm; 
 
-- Создадим  новую таблицу для логов заполнения витрин данных
create schema logs; 
 
-- Создадим витрину оборотов и витрину 101-й отчётной формы
 
-- 1)

drop table if exists dm.dm_account_turnover_f; 

create table dm.dm_account_turnover_f(
on_date date,
account_rk numeric,
credit_amount numeric,
credit_amount_rub numeric(23,8),
debet_amount numeric(23,8),
debet_amount_rub numeric(23,8)
);


-- 2)
 
drop table if exists dm.dm_f101_round_f;

create table dm.dm_f101_round_f(
from_date date,
to_date date,
chapter char(1),
ledger_account char(5),
characteristic char(1),
balance_in_rub numeric(23,8),
r_balance_in_rub numeric(23,8),
balance_in_val numeric(23,8),
r_balance_in_val numeric(23,8),
balance_in_total numeric(23,8),
r_balance_in_total numeric(23,8),
turn_deb_rub numeric(23,8),
r_turn_deb_rub numeric(23,8),
turn_deb_val numeric(23,8),
r_turn_deb_val numeric(23,8),
turn_deb_total numeric(23,8),
r_turn_deb_total numeric(23,8),
turn_cre_rub numeric(23,8),
r_turn_cre_rub numeric(23,8),
turn_cre_val numeric(23,8),
r_turn_cre_val numeric(23,8),
turn_cre_total numeric(23,8),
r_turn_cre_total numeric(23,8),
balance_out_rub numeric(23,8),
r_balance_out_rub numeric(23,8),
balance_out_val numeric(23,8),
r_balance_out_val numeric(23,8),
balance_out_total numeric(23,8),
r_balance_out_total numeric(23,8)
);

-- 3)
drop table if exists dm.lg_messages;


create table dm.lg_messages( 	
		record_id serial,
		date_time timestamp,
		pid varchar,
		message varchar,
		message_type varchar,
		usename varchar, 
		datname varchar, 
		client_addr varchar, 
		application_name varchar,
		backend_start varchar,
		primary key(record_id)
	);

-- Создадим последовательность dm.seq_lg_messages

create sequence if not exists dm.seq_lg_messages;

call dm.fill_account_turnover_f('2018-01-11'::date)


select count(*) from dm.dm_account_turnover_f;

call dm.fill_f101_round_f('2018-01-01'::date)


select * from  dm.dm_f101_round_f
select * from dm.lg_messages

 
/*Если нам нужно запланировать наполнение витрин,
скажем ежедневно или в первый день месяца, следующего за отчетным
то можно воспользоваться планировщиком задач.
Одним из них является планировщик pg_timetable.
Предпочитаю использовать его, так как установка занимает пару минут 
и работает он под Windows  в том числе*/

/*Для нашего задания например, можно запланировать запуск двух 
данных процедур после января, 01/02/ГГГГ или в конце каждого дня января*/

-- Пример. Запустить процедуры 3 января в 23:59  по времени сервера для выбранной даты - 3 января
  
select timetable.add_job('call-dm_account_turnover_f', '59 23 3 1 *', 'call dm.fill_f101_round_f(''2018-01-03''::date)');
  

-- Удаление заданий(действие для определенной даты) или работы(действие) 
-- можно выполнять с помощью соответствующих команд 

select timetable.delete_task(1)
select timetable.delete_job('call-dm_account_turnover_f')
 
select * from timetable.active_chain ac 
select * from timetable.chain c
 
 