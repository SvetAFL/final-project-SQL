-- #TRANSACTIONS & PASSPORTS просто дополняем,
-- #в TERMINALS загружаем инкремент
SET search_path TO final;

insert into final.DWH_FACT_PASSPORT_BLACKLIST (
    date,
    passport)
    select
        date,
        passport
    from stg_passports;
    
insert into final.DWH_FACT_TRANSACTIONS (
    transaction_id,
    transaction_date,
    amount,
    card_num,
    oper_type,
    oper_result,
    terminal)
    select
        transaction_id,
        transaction_date,
        amount,
        card_num,
        oper_type,
        oper_result,
        terminal
	from stg_transactions;  
    

-- создаем представление V_DWH_DIM_terminals_HIST (срез из БД), 
-- чтобы потом сравнивать с новыми данными из stg_terminals
drop view if exists final.v_terminals;
create view final.v_terminals as select
    terminal_id,
    terminal_type,
    terminal_city,
    terminal_address
from final.DWH_DIM_terminals_HIST
where deleted_flg=0
and current_timestamp between effective_from and effective_to;

-- находим новые терминалы. 
-- Создаем таблицу, в которой будут записи, которые есть в stg_terminals, но нет в v_terminals
drop table if exists final.stg_new_terminals;
create table final.stg_new_terminals as
    select 
        t1.terminal_id,
        t1.terminal_type,
        t1.terminal_city,
        t1.terminal_address
    from final.stg_terminals t1
    left join final.v_terminals t2
    on t1.terminal_id = t2.terminal_id
    where t2.terminal_id is null;
    
-- находим изменившиеся терминалы
drop table if exists final.stg_changed_terminals;
create table final.stg_changed_terminals as
    select 
        t1.terminal_id,
        t1.terminal_type,
        t1.terminal_city,
        t1.terminal_address
    from final.stg_terminals t1
    inner join final.v_terminals t2
    on t1.terminal_id = t2.terminal_id
    where t1.terminal_address <> t2.terminal_address;


-- загружаем найденный инкремент
-- новые:
insert into final.DWH_DIM_terminals_HIST (
    terminal_id,
    terminal_type,
    terminal_city,
    terminal_address
    ) select
        terminal_id,
        terminal_type,
        terminal_city,
        terminal_address
    from final.stg_new_terminals;

-- измененные
update final.DWH_DIM_terminals_HIST
    set effective_to = current_timestamp - interval '1 second'
    where terminal_id in (
        select terminal_id from final.stg_changed_terminals)
    and effective_to = '5999-12-31 23:59:59'::timestamp;

insert into final.DWH_DIM_terminals_HIST (
    terminal_id,
    terminal_type,
    terminal_city,
    terminal_address
    ) select
        terminal_id,
        terminal_type,
        terminal_city,
        terminal_address
    from final.stg_changed_terminals;